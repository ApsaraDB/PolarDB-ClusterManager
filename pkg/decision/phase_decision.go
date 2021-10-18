package decision

import (
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"time"
)

type PhaseDecision struct {
	Ha InsAvailableDecision
}

func (d *PhaseDecision) Name() string {
	return "PhaseDecision"
}

func (d *PhaseDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, statusEvent *status.StatusEvent) ([]action.ActionExecutor, error) {
	clusterMeta := m.ClusterMeta
	clusterSpec := m.ClusterSpec
	clusterStatus := s.ClusterStatus

	var actions []action.ActionExecutor

	if statusEvent.Type != status.EngineStatusType {
		return actions, nil
	}
	if !*common.EnableAllAction && statusEvent.CurState[common.EngineEventCategory].Name != status.EngineStatusAlive {
		return actions, nil
	}

	var engineType common.EngineType
	if rwSpec, err := m.GetRwSpec(statusEvent.ClusterID); err != nil {
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
	} else {
		if statusEvent.TimeStamp.Before(phaseStartAt) {
			log.Debugf("Skip status event %v since phase %s start at %s", statusEvent, phase, phaseStartAt)
			return actions, nil
		} else if phase == common.EnginePhaseStopped {
			log.Debugf("Skip status event %v since phase %s", statusEvent, phase)
			return actions, nil
		} else if phase == common.EnginePhasePending ||
			phase == common.EnginePhaseStarting ||
			phase == common.EnginePhaseFailed {

			// 节点加入&重启
			if statusEvent.CurState[common.EngineEventCategory].Name == status.EngineStatusAlive {

				// Only shared storage standby cluster should sync
				if engineType == common.StandbyEngine && insSpec.ClusterType == common.SharedStorageCluster {
					if st, exist := clusterStatus.SubCluster[insSpec.ClusterID].EngineStatus[insSpec.Endpoint][common.EngineDelayEventCategory]; exist && st.Name == status.EngineMetricsReplicaLagNormal {
					} else {
						return actions, nil
					}
				}

				insSpec := *clusterSpec.SubCluster[statusEvent.ClusterID].InstanceSpec[statusEvent.Endpoint.String()]
				actions = append(actions, &action.PhaseChangeActionExecutor{
					Ins:         insSpec,
					EnginePhase: common.EnginePhaseRunning,
					Type:        engineType,
					RemoveTag: map[string]string{
						common.SyncStatusTag: "",
					},
				})

				actions = append(actions, &action.AlarmActionExecutor{
					Alarm: common.AlarmInfo{
						Type: common.AlarmRoFailedType,
						Spec: insSpec,
					},
					Op: common.RemoveAlarm,
				})

				return actions, nil
			}
		}
		if phase == common.EnginePhaseStarting || phase == common.EnginePhaseRunning {
			// 节点不可用
			if available, reason := d.Ha.EvaluateInsAvailable(clusterStatus.SubCluster[statusEvent.ClusterID], insSpec, engineType); !available {
				if !*common.EnableRebuildRoStandby {
					log.Infof("Skip %s change phase from %s to %s since enable_rebuild_ro_standby is false",
						insSpec.String(), phase, common.EnginePhaseFailed)
					return actions, nil
				}
				if phase == common.EnginePhaseFailed {
					return actions, nil
				}

				insSpec := *clusterSpec.SubCluster[statusEvent.ClusterID].InstanceSpec[statusEvent.Endpoint.String()]
				actions = append(actions, &action.PhaseChangeActionExecutor{
					Ins:         insSpec,
					EnginePhase: common.EnginePhaseFailed,
					Type:        engineType,
					Reason:      reason,
				})
				if engineType != common.RwEngine {
					actions = append(actions, &action.AlarmActionExecutor{
						Alarm: common.AlarmInfo{
							AlarmMsg: common.AlarmMsg{
								EventType:   common.AlarmSmsEventType,
								Message:     insSpec.String() + " is " + common.EnginePhaseFailed + " By Down.",
								ClusterName: resource.GetClusterManagerConfig().Cluster.Name,
								Tags: map[string]string{
									"EngineType": string(engineType),
									"Ins":        insSpec.String(),
								},
							},
							Type:    common.AlarmRoFailedType,
							Spec:    insSpec,
							StartAt: time.Now(),
						},
						Op: common.AddAlarm,
					})
				}
				return actions, nil
			}
		}
		// 节点停止
		if phase == common.EnginePhaseStopping {
			if statusEvent.CurState[common.EngineEventCategory].Name != status.EngineStatusInit &&
				statusEvent.CurState[common.EngineEventCategory].Name != status.EngineStatusAlive {
				ac := &action.PhaseChangeActionExecutor{
					Ins:         *clusterSpec.SubCluster[statusEvent.ClusterID].InstanceSpec[statusEvent.Endpoint.String()],
					EnginePhase: common.EnginePhaseStopped,
					Type:        engineType,
				}
				actions = append(actions, ac)
				return actions, nil
			}
		}
	}

	return actions, nil
}
