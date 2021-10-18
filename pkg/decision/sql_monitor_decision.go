package decision

import (
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/collector"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"strings"
	"time"
)

type SQLMonitorDecision struct {
}

func (d *SQLMonitorDecision) Name() string {
	return "SQLMonitorDecision"
}

func (d *SQLMonitorDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, statusEvent *status.StatusEvent) ([]action.ActionExecutor, error) {
	clusterMeta := m.ClusterMeta

	var actions []action.ActionExecutor

	if statusEvent.Type == status.ProxyStatusType {
		return actions, nil
	}
	if !*common.EnableAllAction {
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
		return nil, err
	}

	if phase, _, err := m.GetInsPhase(insSpec); err != nil {
		return nil, err
	} else if phase == common.EnginePhaseRunning {
		if metrics, exist := s.ClusterStatus.SubCluster[rwSpec.ClusterID].EngineStatus[insSpec.Endpoint][common.EngineMetricsEventCategory]; exist {
			for k, v := range metrics.Value {
				if strings.HasPrefix(k, collector.MonitorNamePrefix) {
					if v.(common.MonitorItem).IsAlarm {
						if !m.IsAlarmExist(k, insSpec) {
							actions = append(actions, &action.AlarmActionExecutor{
								Alarm: common.AlarmInfo{
									AlarmMsg: common.AlarmMsg{
										EventType:   common.AlarmSmsEventType,
										Message:     insSpec.String() + " " + k + " is alarm! msg: " + v.(common.MonitorItem).Msg,
										ClusterName: resource.GetClusterManagerConfig().Cluster.Name,
										Tags: map[string]string{
											"EngineType": string(engineType),
											"Ins":        insSpec.String(),
										},
									},
									Type:    k,
									Spec:    *insSpec,
									StartAt: time.Now(),
								},
								Op: common.AddAlarm,
							})
						}
					} else {
						if m.IsAlarmExist(k, insSpec) {
							actions = append(actions, &action.AlarmActionExecutor{
								Alarm: common.AlarmInfo{
									Type: k,
									Spec: *insSpec,
								},
								Op: common.RemoveAlarm,
							})
						}
					}

				}

			}
		}
	}

	return actions, nil
}
