package action

import (
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type PhaseChangeActionExecutor struct {
	Ins         common.InsSpec
	EnginePhase string
	AddTag      map[string]string
	RemoveTag   map[string]string
	Type        common.EngineType
	Reason      common.HaReason
}

func (e *PhaseChangeActionExecutor) String() string {
	return "PhaseChangeActionExecutor: ins: " + e.Ins.String() + " type: " + string(e.Type) + " phase: " + e.EnginePhase
}

func (e *PhaseChangeActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	phase, _, err := metaManager.GetInsPhase(&e.Ins)
	if err != nil {
		log.Warnf("Failed to get %s phase err:%s", e.Ins.String(), err.Error())
		return err
	}

	rEvent := notify.BuildDBEventPrefixByInsV2(&e.Ins, e.Type, e.Ins.Endpoint)
	if e.EnginePhase == common.EnginePhaseFailed {
		rEvent.Body.Describe = fmt.Sprintf("instance %v phase change decision, po: %v, phase from %s to %s, reason: %s",
			e.Reason.String(), e.Ins.CustID, e.Ins.PodName, phase, e.EnginePhase)
	} else {
		rEvent.Body.Describe = fmt.Sprintf("instance %v phase change decision, po: %v, phase from %s to %s", e.Ins.CustID, e.Ins.PodName, phase, e.EnginePhase)
	}
	rEvent.EventCode = notify.EventCode_PhaseChange

	if e.EnginePhase == common.EnginePhaseFailed || e.EnginePhase == common.EnginePhaseStopped || e.EnginePhase == common.EnginePhaseStopping {
		rEvent.Level = notify.EventLevel_ERROR
	} else if e.EnginePhase == common.EnginePhasePending {
		rEvent.Level = notify.EventLevel_WARN
	} else {
		rEvent.Level = notify.EventLevel_INFO
	}
	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("PhaseChangeActionExecutor err: %v", sErr)
	}

	switch phase {
	case common.EnginePhasePending:
		fallthrough
	case common.EnginePhaseStarting:
		switch e.EnginePhase {
		case common.EnginePhaseRunning:
			fallthrough
		case common.EnginePhaseFailed:
			err := resource.GetResourceManager().GetOperatorClient().UpdateEngineStatus(e.Ins, e.EnginePhase, e.Reason.String())
			if err != nil {
				if resource.IsPolarSharedMode() {
					return errors.Wrapf(err, "Failed to update %s status cluster:%s id:%s state:%s err:%s",
						e.Type, e.Ins.ClusterID, e.Ins.ID, e.EnginePhase, err.Error())
				}
			}

			if err := metaManager.SetInsPhase(&e.Ins, e.EnginePhase); err != nil {
				return err
			}
			metaManager.AddEngineTag(&e.Ins, e.AddTag)
			metaManager.RemoveEngineTag(&e.Ins, e.RemoveTag)
			ExecuteWithRetry(metaManager.Sync, 10, true)
		default:
			return errors.Errorf("Failed to switch phase from %s to %s", phase, e.EnginePhase)
		}
	case common.EnginePhaseRunning:
		switch e.EnginePhase {
		case common.EnginePhaseFailed:
			err := resource.GetResourceManager().GetOperatorClient().UpdateEngineStatus(e.Ins, e.EnginePhase, e.Reason.String())
			if err != nil {
				if resource.IsPolarSharedMode() {
					return errors.Wrapf(err, "Failed to update %s status cluster:%s id:%s state:%s err:%s",
						e.Type, e.Ins.ClusterID, e.Ins.ID, e.EnginePhase, err.Error())
				}
			}
			if err := metaManager.SetInsPhase(&e.Ins, e.EnginePhase); err != nil {
				return err
			}
			metaManager.AddEngineTag(&e.Ins, e.AddTag)
			metaManager.RemoveEngineTag(&e.Ins, e.RemoveTag)
			ExecuteWithRetry(metaManager.Sync, 10, true)
		case common.EnginePhaseRunning:
			metaManager.AddEngineTag(&e.Ins, e.AddTag)
			metaManager.RemoveEngineTag(&e.Ins, e.RemoveTag)
			ExecuteWithRetry(metaManager.Sync, 10, true)
		default:
			return errors.Errorf("Failed to switch phase from %s to %s", phase, e.EnginePhase)
		}
	case common.EnginePhaseStopping:
		switch e.EnginePhase {
		case common.EnginePhaseStopped:
			statusManager.DeleteEngineStatus(e.Ins.Endpoint)
			fallthrough
		case common.EnginePhaseRunning:
			err := resource.GetResourceManager().GetOperatorClient().UpdateEngineStatus(e.Ins, e.EnginePhase, "")
			if err != nil {
				if resource.IsPolarSharedMode() {
					return errors.Wrapf(err, "Failed to update %s status cluster:%s id:%s state:%s err:%s",
						e.Type, e.Ins.ClusterID, e.Ins.ID, e.EnginePhase, err.Error())
				}
			}
			if err := metaManager.SetInsPhase(&e.Ins, e.EnginePhase); err != nil {
				return err
			}
			metaManager.AddEngineTag(&e.Ins, e.AddTag)
			metaManager.RemoveEngineTag(&e.Ins, e.RemoveTag)
			ExecuteWithRetry(metaManager.Sync, 10, true)
		default:
			return errors.Errorf("Failed to switch phase from %s to %s", phase, e.EnginePhase)
		}
	case common.EnginePhaseFailed:
		switch e.EnginePhase {
		case common.EnginePhaseRunning:
			err := resource.GetResourceManager().GetOperatorClient().UpdateEngineStatus(e.Ins, e.EnginePhase, "")
			if err != nil {
				if resource.IsPolarSharedMode() {
					return errors.Wrapf(err, "Failed to update %s status cluster:%s id:%s state:%s err:%s",
						e.Type, e.Ins.ClusterID, e.Ins.ID, e.EnginePhase, err.Error())
				}
			}
			if err := metaManager.SetInsPhase(&e.Ins, e.EnginePhase); err != nil {
				return err
			}
			metaManager.AddEngineTag(&e.Ins, e.AddTag)
			metaManager.RemoveEngineTag(&e.Ins, e.RemoveTag)
			ExecuteWithRetry(metaManager.Sync, 10, true)
		case common.EnginePhaseFailed:
			metaManager.AddEngineTag(&e.Ins, e.AddTag)
			metaManager.RemoveEngineTag(&e.Ins, e.RemoveTag)
			ExecuteWithRetry(metaManager.Sync, 10, true)
		default:
			return errors.Errorf("Failed to switch phase from %s to %s", phase, e.EnginePhase)
		}
	default:
		return errors.Errorf("Failed to switch phase from %s to %s", phase, e.EnginePhase)
	}

	log.Infof("Success to update %s %s status to %s with tag %v", e.Type, e.Ins.String(), e.EnginePhase, e.AddTag)
	return nil
}
