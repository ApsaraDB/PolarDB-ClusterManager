package action

import (
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"strings"
	"time"
)

type MasterStandbySwitchActionExecutor struct {
	SwitchActionExecutor
}

func (e *MasterStandbySwitchActionExecutor) String() string {
	return "MasterStandbySwitchActionExecutor: " + e.SwitchAction.String()
}

func (a *MasterStandbySwitchActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	startTs := time.Now()
	log.Infof("Start Switch from %s to %s", a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String())

	err := ExecuteWithRetry(a.StartSwitchTask, 3, false)
	if err != nil {
		return err
	}
	prePhase := metaManager.GetPhase()
	metaManager.SwitchPhase(common.SwitchingPhase)
	if metaManager.ClusterMeta.MasterClusterID == a.SwitchAction.NewRw.ClusterID {
		log.Infof("Success Switch %s already master", a.SwitchAction.NewRw.ClusterID)
	} else if metaManager.ClusterMeta.MasterClusterID != a.SwitchAction.OldRw.ClusterID {
		log.Infof("Skip Switch old master %s already change to %s", a.SwitchAction.OldRw.ClusterID, metaManager.GetMasterClusterID())
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
		log.Infof("Failed Switch from %s to %s err %s decision %s cost %s",
			a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String(), err.Error(), a.Event.Reason.DecisionTime, a.Event.SwitchTime.String())
		return err
	} else {
		log.Infof("Success Switch from %s to %s decision %s rto %s cost %s",
			a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String(), a.Event.Reason.DecisionTime, a.Rto.String(), time.Since(startTs).String())
		return nil
	}

	return nil
}

func (a *MasterStandbySwitchActionExecutor) TerminateBackend() error {
	startTs := time.Now()
	var err error = nil
	l := fmt.Sprintf("terminate %s backend", a.SwitchAction.OldRw.String())
	defer a.LogSwitchEvent("TerminateBackend", startTs, l, &err)

	db := resource.GetResourceManager().GetEngineConn(a.SwitchAction.OldRw.Endpoint)
	res, err := db.Exec(common.TerminateBackendSQL)
	if err != nil {
		return errors.Wrapf(err, "Failed to terminate backend %s sql %s", a.SwitchAction.OldRw.String(), common.TerminateBackendSQL)
	}
	a.Event.AffectedSessions = res.RowsReturned()

	return nil
}

func (a *MasterStandbySwitchActionExecutor) LockReadonly(readonly bool) error {
	startTs := time.Now()
	var err error
	// 设置节点只读
	e := resource.GetResourceManager().GetEngineManager(a.SwitchAction.OldRw.Endpoint)
	if readonly {
		err = e.LockReadonly(3)
	} else {
		err = e.UnlockReadonly(3)
	}

	l := fmt.Sprintf("lock %s to %v", a.SwitchAction.OldRw.String(), readonly)
	a.LogSwitchEvent("LockMaster", startTs, l, &err)

	return err
}

func (a *MasterStandbySwitchActionExecutor) WaitMasterStandbySync() error {
	startTs := time.Now()
	var err error = nil
	db := resource.GetResourceManager().GetEngineConn(a.SwitchAction.OldRw.Endpoint)
	standby := &a.SwitchAction.NewRw
	slotName := `standby_` + standby.CustID + "_" + strings.Split(standby.PodName, "-")[len(strings.Split(standby.PodName, "-"))-1]
	l := fmt.Sprintf("wait master %s standby %s sync", a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String())
	defer a.LogSwitchEvent("WaitMasterStandbySync", startTs, l, &err)

	time.Sleep(time.Millisecond * time.Duration(*common.EngineDetectIntervalMs))

	type ReplicationStat struct {
		PgCurrentWalLsn string
		WriteLsn        string
	}
	for {
		var replicationStat []ReplicationStat
		sql := common.InternalMarkSQL + `select pg_current_wal_lsn, write_lsn from pg_stat_replication where application_name='` + slotName + `'`
		_, err = db.Query(&replicationStat, sql)
		if err != nil {
			return errors.Errorf("Failed to query pg_current_wal_lsn/write_lsn, sql %s", sql)
		}
		if len(replicationStat) != 1 {
			return errors.Errorf("Failed to query pg_current_wal_lsn/write_lsn, sql %s replicationStat size %d", sql, len(replicationStat))
		}
		if replicationStat[0].PgCurrentWalLsn == replicationStat[0].WriteLsn {
			break
		}
		if int(time.Since(startTs).Seconds()) > a.SwitchAction.RTO {
			err = errors.Errorf("Failed to wait master standby sync util %d second, pg_current_wal_lsn:%s write_lsn%s", a.SwitchAction.RTO, replicationStat[0].PgCurrentWalLsn, replicationStat[0].WriteLsn)
			return err
		}
		time.Sleep(time.Millisecond * 50)
	}

	return nil
}

func (a *MasterStandbySwitchActionExecutor) ExecuteSwitch(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	err := ExecuteWithRetry(a.SwitchRwVip, 3, false)
	if err != nil {
		// rollback, ignore error cause vip check will recover it
		ExecuteWithRetry(a.RollbackRwVip, 3, false)
		log.Warnf("Failed to SwitchRwVip, err=%s", err.Error())
		return err
	}

	if a.SwitchAction.Manual {
		err := a.Checkpoint()
		if err != nil {
			log.Warnf("Failed to Checkpoint, err=%s", err.Error())
			if !a.SwitchAction.Force {
				return err
			}
		}

		a.Event.Reason.RtoStart = time.Now()

		err = a.LockReadonly(true)
		if err != nil {
			a.LockReadonly(false)
			return err
		}

		err = ExecuteWithRetry(a.TerminateBackend, 3, false)
		if err != nil {
			if a.SwitchAction.Force {
				log.Infof("Skip terminate backend since force switch")
			} else {
				return err
			}
		}

		err = a.WaitMasterStandbySync()
		if err != nil {
			if a.SwitchAction.Force {
				log.Infof("Skip wait master standby sync since force switch")
			} else {
				a.LockReadonly(false)
				return err
			}
		}
	}

	if !resource.IsPolarPaasMode() {
		err = ExecuteWithRetry(a.CleanAndCreateSlot, 3, false)
		if err != nil {
			if a.SwitchAction.Manual {
				a.LockReadonly(false)
			}
			return err
		}
	}

	err = ExecuteWithRetry(a.SwitchRwVip, 3, false)
	if err != nil {
		// rollback, ignore error cause vip check will recover it
		if a.SwitchAction.Manual {
			a.LockReadonly(false)
		}
		ExecuteWithRetry(a.RollbackRwVip, 3, false)
		log.Warnf("Failed to CleanAndCreateSlot, err=%s", err.Error())
		return err
	}

	if a.SwitchAction.Manual {
		a.LockReadonly(false)
	}

	stopOldRwErr := make(chan error)
	go func() {
		err := a.StopOldRw()
		if err != nil {
			log.Warnf("Failed to stop old rw err=%s", err)
		}
		stopOldRwErr <- err
	}()

	if a.SwitchAction.Manual {
		err := <-stopOldRwErr
		if err != nil {
			a.StartOldRw()
			ExecuteWithRetry(a.RollbackRwVip, 3, false)
			return err
		}
	}

	err = a.PromoteNewRw(common.RwEngine)
	if err != nil {
		log.Warnf("Failed to promote new rw err=%s", err.Error())
	}

	err = a.WaitRwReady()
	if err != nil {
		log.Warnf("Failed to wait new rw err=%s", err.Error())
	}

	a.Rto = time.Since(a.Event.Reason.RtoStart)
	a.Event.RTO = a.Rto

	for vip, _ := range metaManager.Vip {
		spec := metaManager.Vip[vip]
		spec.Endpoint = a.SwitchAction.NewRw.Endpoint.String()
		metaManager.Vip[vip] = spec
	}

	if !a.SwitchAction.Manual {
		<-stopOldRwErr
	}

	err = a.DemoteOldRw(common.StandbyEngine)
	if err != nil {
		log.Warnf("Failed to demote old rw err=%s", err.Error())
	}

	err = a.ReConfigStandby()
	if err != nil {
		log.Warnf("Failed to config standby err=%s", err.Error())
	}

	ExecuteWithRetry(a.UpdateK8sStatus, 30, true)

	metaManager.ClusterMeta.MasterClusterID = a.SwitchAction.NewRw.ClusterID

	return nil
}
