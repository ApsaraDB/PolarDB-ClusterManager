package action

import (
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"time"
)

type StandbyReplicaSwitchActionExecutor struct {
	SwitchActionExecutor
}

func (e *StandbyReplicaSwitchActionExecutor) String() string {
	return "StandbyReplicaSwitchActionExecutor: " + e.SwitchAction.String()
}

func (a *StandbyReplicaSwitchActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	startTs := time.Now()
	log.Infof("Start Standby Switch from %s to %s", a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String())

	err := ExecuteWithRetry(a.StartSwitchTask, 3, false)
	if err != nil {
		return err
	}

	prePhase := metaManager.GetPhase()
	metaManager.SwitchPhase(common.SwitchingPhase)
	if metaManager.ClusterMeta.SubCluster[a.SwitchAction.NewRw.ClusterID].RwEndpoint == a.SwitchAction.NewRw.Endpoint {
		log.Infof("Success Switch %s already rw", a.SwitchAction.NewRw.Endpoint)
	} else if metaManager.ClusterMeta.SubCluster[a.SwitchAction.OldRw.ClusterID].RwEndpoint != a.SwitchAction.OldRw.Endpoint {
		log.Infof("Skip Switch old rw %s already change to %s",
			a.SwitchAction.OldRw.Endpoint, metaManager.ClusterMeta.SubCluster[metaManager.ClusterMeta.MasterClusterID].RwEndpoint)
	} else {
		a.DisableSwitchInsStatus(metaManager, statusManager)
		err = a.ExecuteSwitch(metaManager, statusManager)
		a.EnableSwitchInsStatus(metaManager, statusManager)
		ExecuteWithRetry(metaManager.Sync, 10, true)
	}
	if prePhase == common.SwitchingPhase {
		metaManager.SwitchPhase(common.RunningPhase)
	} else {
		metaManager.SwitchPhase(prePhase)
	}
	ExecuteWithRetry(a.FinishSwitchTask, 3, true)

	if err != nil {
		log.Infof("Failed Switch from %s to %s err %s cost %s", a.SwitchAction.OldRw.Endpoint, a.SwitchAction.NewRw.Endpoint, err.Error(), time.Since(startTs).String())
		return err
	} else {
		log.Infof("Success Switch from %s to %s cost %s", a.SwitchAction.OldRw.Endpoint, a.SwitchAction.NewRw.Endpoint, time.Since(startTs).String())
		return nil
	}

	return nil
}

func (a *StandbyReplicaSwitchActionExecutor) ExecuteSwitch(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {

	err := ExecuteWithRetry(a.CleanAndCreateSlot, 3, false)
	if err != nil {
		log.Warnf("Failed to CleanAndCreateSlot, err=%s", err.Error())
		return err
	}

	// checkpoint: optimize for manual switchover
	if a.SwitchAction.Manual {
		err = a.Checkpoint()
		if err != nil && !a.SwitchAction.Force {
			log.Warnf("Failed to Checkpoint, err=%s", err.Error())
			return err
		}
	}

	err = ExecuteWithTimeoutRetry(a.SwitchStore, 3, 7*time.Second, false)
	if err != nil {
		log.Warnf("Failed to switch store err=%s", err.Error())
		ExecuteWithTimeoutRetry(a.RollbackStore, 3, 7*time.Second, false)
		return err
	}

	err = a.DemoteOldRw(common.RoEngine)
	if err != nil {
		log.Warnf("Failed to demote old rw err=%s", err)
	}

	err = a.PromoteNewStandby(a.SwitchAction.Master)
	if err != nil {
		log.Warnf("Failed to promote new rw err=%s", err.Error())
	}

	err = a.ReConfigRo()
	if err != nil {
		log.Warnf("Failed to config ro err=%s", err.Error())
	}

	ExecuteWithRetry(a.UpdateK8sStatus, 30, true)

	metaManager.ClusterMeta.SubCluster[a.SwitchAction.NewRw.ClusterID].RwEndpoint = a.SwitchAction.NewRw.Endpoint

	return nil
}
