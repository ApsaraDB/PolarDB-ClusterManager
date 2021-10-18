package decision

import (
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"strconv"
	"time"
)

type StandbyDelayDecision struct {
}

func (d *StandbyDelayDecision) Name() string {
	return "StandbyDelayDecision"
}

func (d *StandbyDelayDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, statusEvent *status.StatusEvent) ([]action.ActionExecutor, error) {
	clusterMeta := m.ClusterMeta
	clusterSpec := m.ClusterSpec

	var actions []action.ActionExecutor

	if statusEvent.Type == status.ProxyStatusType {
		return actions, nil
	}
	if !*common.EnableAllAction {
		return actions, nil
	}
	if !*common.EnableRebuildRoStandby {
		return actions, nil
	}

	var engineType common.EngineType
	rwSpec, err := m.GetRwSpec(statusEvent.ClusterID)
	if err != nil {
		return nil, err
	} else if rwSpec.Endpoint == statusEvent.Endpoint {
		if statusEvent.ClusterID == clusterMeta.MasterClusterID {
			engineType = common.RwEngine
		} else {
			engineType = common.StandbyEngine
		}
	} else {
		engineType = common.RoEngine
	}

	insSpec, err := m.GetInsSpec(&statusEvent.Endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "status event %v", statusEvent)
	}

	if phase, phaseStartAt, err := m.GetInsPhase(insSpec); err != nil {
		return nil, err
	} else if engineType == common.StandbyEngine &&
		(phase == common.EnginePhaseFailed || phase == common.EnginePhaseRunning) &&
		statusEvent.TimeStamp.After(phaseStartAt) {

		insSpec := clusterSpec.SubCluster[statusEvent.ClusterID].InstanceSpec[statusEvent.Endpoint.String()]

		if statusEvent.CurState[common.EngineDelayEventCategory].Name == status.EngineMetricsReplicaLagDelay {
			standbyDelaySize, hasDelaySize := statusEvent.CurState[common.EngineMetricsEventCategory].Value[common.EventReplayDelaySize].(int)
			if !hasDelaySize {
				standbyDelaySize = -1
			}
			standbyDelayTime, hasDelayTime := statusEvent.CurState[common.EngineMetricsEventCategory].Value[common.EventReplayDelayTime].(int)
			if !hasDelayTime {
				standbyDelayTime = -1
			}

			log.Infof("Standby %s delay time:%d data:%d", statusEvent.Endpoint.String(), standbyDelayTime, standbyDelaySize)

			if tag := m.GetEngineTag(insSpec); tag != nil {
				if v, exist := tag[common.SyncStatusTag]; exist && phase == common.EnginePhaseFailed && v == common.SyncStatusUnSync {
					return actions, nil
				}
			}

			ac := &action.PhaseChangeActionExecutor{
				Ins:         *insSpec,
				EnginePhase: common.EnginePhaseFailed,
				Type:        engineType,
				AddTag: map[string]string{
					common.SyncStatusTag: common.SyncStatusUnSync},
			}

			actions = append(actions, ac)

			actions = append(actions, &action.AlarmActionExecutor{
				Alarm: common.AlarmInfo{
					AlarmMsg: common.AlarmMsg{
						EventType: common.AlarmSmsEventType,
						Message: insSpec.String() + " is " + common.EnginePhaseFailed + " By Standby Delay data: " +
							strconv.Itoa(standbyDelaySize) + " time: " + strconv.Itoa(standbyDelayTime),
						ClusterName: resource.GetClusterManagerConfig().Cluster.Name,
						Tags: map[string]string{
							"EngineType": string(engineType),
							"Ins":        insSpec.String(),
						},
					},
					Type:    common.AlarmRoDelay,
					Spec:    *insSpec,
					StartAt: time.Now(),
				},
				Op: common.AddAlarm,
			})
		}

	}

	return actions, nil
}
