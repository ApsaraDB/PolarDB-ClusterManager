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

type RoDelayDecision struct {
}

func (d *RoDelayDecision) Name() string {
	return "RoDelayDecision"
}

func (d *RoDelayDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, statusEvent *status.StatusEvent) ([]action.ActionExecutor, error) {
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
	} else if engineType == common.RoEngine &&
		(phase == common.EnginePhaseFailed || phase == common.EnginePhaseRunning) &&
		statusEvent.TimeStamp.After(phaseStartAt) {
		// rw buffer满
		var fatal bool
		if v, exist := s.ClusterStatus.SubCluster[rwSpec.ClusterID].EngineStatus[rwSpec.Endpoint][common.EngineMetricsEventCategory].Value[common.EventCopyBufferUsage]; exist {
			if v.(int) > *common.EngineBufferUsageFatalRatio {
				log.Infof("%s usage %d exceed fatal %d", common.EventCopyBufferUsage, v.(int), *common.EngineBufferUsageFatalRatio)
				fatal = true
			}
		}
		if v, exist := s.ClusterStatus.SubCluster[rwSpec.ClusterID].EngineStatus[rwSpec.Endpoint][common.EngineMetricsEventCategory].Value[common.EventSharedBufferUsage]; exist {
			if v.(int) > *common.EngineBufferUsageFatalRatio {
				log.Infof("%s usage %d exceed fatal %d", common.EventSharedBufferUsage, v.(int), *common.EngineBufferUsageFatalRatio)
				fatal = true
			}
		}

		if statusEvent.CurState[common.EngineDelayEventCategory].Name == status.EngineMetricsReplicaLagDelay {
			roDelaySize, hasDelaySize := statusEvent.CurState[common.EngineMetricsEventCategory].Value[common.EventDelaySize].(int)
			if !hasDelaySize {
				roDelaySize = -1
			}
			roDelayTime, hasDelayTime := statusEvent.CurState[common.EngineMetricsEventCategory].Value[common.EventDelayTime].(int)
			if !hasDelayTime {
				roDelayTime = -1
			}
			if hasDelaySize && hasDelayTime {
				if fatal {
					if checkpointDelaySize, exist := s.ClusterStatus.SubCluster[rwSpec.ClusterID].EngineStatus[rwSpec.Endpoint][common.EngineMetricsEventCategory].Value[common.EventMaxRecoverySize]; exist {
						if float64(checkpointDelaySize.(int)-roDelaySize)/float64(checkpointDelaySize.(int)) < float64(100-*common.EngineBufferUsageFatalRatio)/100 {
							ac := &action.DropRoActionExecutor{
								InsSpec:   *clusterSpec.SubCluster[statusEvent.ClusterID].InstanceSpec[statusEvent.Endpoint.String()],
								RwInsSpec: *rwSpec,
							}
							actions = append(actions, ac)
						}
						log.Infof("Ro %s delay time:%d data:%d checkpoint delay:%d threshold:%d", statusEvent.Endpoint.String(), roDelayTime, roDelaySize, checkpointDelaySize, *common.EngineBufferUsageFatalRatio)
					}
				} else {
					log.Infof("Ro %s delay time:%d data:%d", statusEvent.Endpoint.String(), roDelayTime, roDelaySize)
				}
			}

			insSpec := clusterSpec.SubCluster[statusEvent.ClusterID].InstanceSpec[statusEvent.Endpoint.String()]
			if tag := m.GetEngineTag(insSpec); tag != nil {
				if v, exist := tag[common.SyncStatusTag]; exist && v == common.SyncStatusUnSync {
					return actions, nil
				}
			}

			ac := &action.PhaseChangeActionExecutor{
				Ins:         *insSpec,
				EnginePhase: phase,
				Type:        engineType,
				AddTag: map[string]string{
					common.SyncStatusTag: common.SyncStatusUnSync},
			}

			actions = append(actions, ac)

			actions = append(actions, &action.AlarmActionExecutor{
				Alarm: common.AlarmInfo{
					AlarmMsg: common.AlarmMsg{
						EventType: common.AlarmSmsEventType,
						Message: insSpec.String() + " is " + phase + " By Ro Delay date: " +
							strconv.Itoa(roDelaySize) + " time: " + strconv.Itoa(roDelayTime),
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
		} else if statusEvent.CurState[common.EngineDelayEventCategory].Name == status.EngineMetricsReplicaLagNormal {
			insSpec := clusterSpec.SubCluster[statusEvent.ClusterID].InstanceSpec[statusEvent.Endpoint.String()]
			if tag := m.GetEngineTag(insSpec); tag != nil {
				if v, exist := tag[common.SyncStatusTag]; exist && v == common.SyncStatusUnSync {
					ac := &action.PhaseChangeActionExecutor{
						Ins:         *insSpec,
						EnginePhase: phase,
						Type:        engineType,
						RemoveTag: map[string]string{
							common.SyncStatusTag: common.SyncStatusUnSync},
					}

					actions = append(actions, ac)
				}
			}
		} else {
			return actions, nil
		}

	}

	return actions, nil
}
