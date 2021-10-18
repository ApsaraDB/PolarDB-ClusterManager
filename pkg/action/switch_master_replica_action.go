package action

import (
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"sync"
	"time"
)

type MasterReplicaSwitchActionExecutor struct {
	SwitchActionExecutor
}

func (a *MasterReplicaSwitchActionExecutor) String() string {
	return "MasterReplicaSwitchActionExecutor: " + a.SwitchAction.String()
}

func (a *MasterReplicaSwitchActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	startTs := time.Now()
	log.Infof("Start Switch from %s to %s", a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String())

	prePhase := metaManager.GetPhase()
	metaManager.SwitchPhase(common.SwitchingPhase)
	err := ExecuteWithRetry(a.StartSwitchTask, 3, false)
	if err != nil {
		return err
	}
	if metaManager.ClusterMeta.SubCluster[metaManager.ClusterMeta.MasterClusterID].RwEndpoint == a.SwitchAction.NewRw.Endpoint {
		log.Infof("Success Switch %s already rw", a.SwitchAction.NewRw.Endpoint)
	} else if metaManager.ClusterMeta.SubCluster[metaManager.ClusterMeta.MasterClusterID].RwEndpoint != a.SwitchAction.OldRw.Endpoint {
		log.Infof("Skip Switch old rw %s already change to %s",
			a.SwitchAction.OldRw.Endpoint, metaManager.ClusterMeta.SubCluster[metaManager.ClusterMeta.MasterClusterID].RwEndpoint)
	} else {
		a.DisableSwitchInsStatus(metaManager, statusManager)
		err = a.ExecuteSwitch(metaManager, statusManager)
		a.EnableSwitchInsStatus(metaManager, statusManager)
		synErr := ExecuteWithRetry(metaManager.Sync, 10, true)
		if synErr != nil {
			log.Errorf("Switch old rw %s change to %s : err:%v",
				a.SwitchAction.OldRw.Endpoint, metaManager.ClusterMeta.SubCluster[metaManager.ClusterMeta.MasterClusterID].RwEndpoint, synErr)
		}
	}
	if prePhase == common.SwitchingPhase {
		metaManager.SwitchPhase(common.RunningPhase)
	} else {
		metaManager.SwitchPhase(prePhase)
	}
	ExecuteWithRetry(a.FinishSwitchTask, 3, true)

	if err != nil {
		log.Infof("Failed Switch from %s to %s err %s decision %s cost %s",
			a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String(), err.Error(), a.Event.Reason.DecisionTime, a.Event.SwitchTime.String())
		return err
	} else {
		log.Infof("Success Switch from %s to %s decision %s rto %s cost %s",
			a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String(), a.Event.Reason.DecisionTime, a.Rto.String(), time.Since(startTs).String())
		return nil
	}
}

func (a *MasterReplicaSwitchActionExecutor) ExecuteSwitch(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {

	// checkpoint: optimize for manual switchover
	if a.SwitchAction.Manual {
		err := a.Checkpoint()
		if err != nil {
			log.Warnf("Failed to Checkpoint, err=%s", err.Error())
			if !a.SwitchAction.Force {
				return err
			}
		}
	}

	err := ExecuteWithRetry(a.CheckEngineVersion, 3, false)
	if err != nil {
		log.Warnf("Failed to CheckEngineVersion, err=%s", err.Error())
		return err
	}

	createSlot := false
	if !a.PersistSlot {
		err = ExecuteWithRetry(a.CleanAndCreateSlot, 3, false)
		if err != nil {
			log.Warnf("Failed to CleanAndCreateSlot before promote, err=%s", err.Error())
			if !a.SwitchAction.Force {
				return err
			}
		} else {
			createSlot = true
		}
	}

	if a.SwitchAction.Manual {
		a.Event.Reason.RtoStart = time.Now()
	}

	err = a.ParallelVipStoreSwitch()
	if err != nil {
		return err
	}

	a.PromoteRw()

	if a.PersistSlot {
		err := ExecuteWithRetry(a.CleanAndCreateSlotForOnlinePromote, 3, false)
		if err != nil {
			log.Warnf("Failed to CleanAndCreateSlotForOnlinePromote, err=%s", err.Error())
			if !a.SwitchAction.Force {
				return err
			}
		}
	} else {
		if !createSlot {
			err := ExecuteWithRetry(a.CleanAndCreateSlot, 3, false)
			if err != nil {
				log.Warnf("Failed to CleanAndCreateSlot, err=%s", err.Error())
			}
		}
	}

	a.Rto = time.Since(a.Event.Reason.RtoStart)
	a.Event.RTO = a.Rto

	a.ParallelDemoteRoStandby()

	ExecuteWithRetry(a.UpdateK8sStatus, 30, true)

	metaManager.ClusterMeta.SubCluster[a.SwitchAction.NewRw.ClusterID].RwEndpoint = a.SwitchAction.NewRw.Endpoint

	return nil
}

func (a *MasterReplicaSwitchActionExecutor) ParallelVipStoreSwitch() error {
	waitGroup := &sync.WaitGroup{}
	switchErr := make(chan error, 64)

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		err := ExecuteWithRetry(a.SwitchRwVip, 3, false)
		if err != nil {
			log.Warnf("Failed to SwitchRwVip, err=%s", err.Error())
			switchErr <- err
		}
	}()

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		// stop old rw before switch store
		if a.SwitchAction.Manual {
			err := a.StopOldRw()
			if err != nil {
				log.Warnf("Failed to stop old rw, err=%s", err.Error())
			}
		}

		err := ExecuteWithTimeoutRetry(a.SwitchStore, 3, 7*time.Second, false)
		if err != nil {
			log.Warnf("Failed to switch store err=%s", err.Error())
			switchErr <- err
		}
	}()

	waitGroup.Wait()
	close(switchErr)

	for err := range switchErr {
		// rollback, ignore error cause vip check will recover it
		ExecuteWithRetry(a.RollbackRwVip, 3, false)
		ExecuteWithTimeoutRetry(a.RollbackStore, 3, 7*time.Second, false)
		a.StartOldRw()
		return errors.Errorf("Failed to switch vip/store err %s", err.Error())
	}

	return nil
}

func (a *MasterReplicaSwitchActionExecutor) PromoteRw() {
	waitGroup := &sync.WaitGroup{}

	if a.SwitchAction.Manual {
		a.StopAllRo()
	} else {
		waitGroup.Add(1)
		go func() {
			a.StopAllRo()
			waitGroup.Done()
		}()
	}

	err := a.PromoteNewRw(common.RwEngine)
	if err != nil {
		log.Warnf("Failed to promote new rw err=%s", err.Error())
	}

	a.WaitRwReady()

	if !a.SwitchAction.Manual {
		waitGroup.Wait()
	}
}

func (a *MasterReplicaSwitchActionExecutor) ParallelDemoteRoStandby() {
	waitGroup := &sync.WaitGroup{}

	waitGroup.Add(1)
	go func() {
		err := a.ReConfigStandby()
		if err != nil {
			log.Warnf("Failed to config standby err=%s", err.Error())
		}
		waitGroup.Done()
	}()

	waitGroup.Add(1)
	go func() {
		err := a.ReConfigRo()
		if err != nil {
			log.Warnf("Failed to config ro err=%s", err.Error())
		}
		waitGroup.Done()
	}()

	waitGroup.Add(1)
	go func() {
		err := a.DemoteOldRw(common.RoEngine)
		if err != nil {
			log.Warnf("Failed to demote old rw err=%s", err.Error())
		}

		err = ExecuteWithTimeoutRetry(a.CleanOldRwSlot, 3, 10*time.Second, false)
		if err != nil {
			log.Warnf("Failed to clean old rw slot err=%s", err.Error())
		}
		waitGroup.Done()
	}()

	waitGroup.Wait()
}
