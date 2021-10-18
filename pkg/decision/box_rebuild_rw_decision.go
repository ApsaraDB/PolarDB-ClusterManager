package decision

import (
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"k8s.io/api/core/v1"
)

type BoxRebuildRwDecision struct {
	haDecision InsAvailableDecision
	meta       *meta.MetaManager
}

func (d *BoxRebuildRwDecision) Name() string {
	return "BoxRebuildRwDecision"
}

func (d *BoxRebuildRwDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, statusEvent *status.StatusEvent) ([]action.ActionExecutor, error) {
	if !*common.EnableAllAction {
		return nil, nil
	}
	if common.IsHaManualMode() {
		return nil, nil
	}

	d.meta = m

	var actions []action.ActionExecutor

	if subStatus, exist := s.ClusterStatus.SubCluster[m.ClusterMeta.MasterClusterID]; exist {
		// 判断RW节点可用性
		rwSpec, err := m.GetRwSpec(m.ClusterMeta.MasterClusterID)
		if err != nil {
			log.Fatalf("Failed to get rw spec, err=%s", err.Error())
		}
		if phase, ts := m.GetRwEnginePhase(); statusEvent.TimeStamp.After(ts) {
			// 通过用户链路连接判断
			if available, reason := d.haDecision.EvaluateInsAvailable(subStatus, rwSpec, common.RwEngine); phase == common.EnginePhaseRunning && !available {
				log.Debugf("BoxRebuildRwDecision CurStatus: %s", s.ClusterStatus.String(nil))
				ac := &action.PhaseChangeActionExecutor{
					Ins:         *m.ClusterSpec.SubCluster[statusEvent.ClusterID].InstanceSpec[statusEvent.Endpoint.String()],
					EnginePhase: common.EnginePhaseFailed,
					Type:        common.RwEngine,
					Reason:      reason,
				}
				actions = append(actions, ac)
			} else if statusEvent.CurState[common.EngineEventCategory].Name == status.EngineStatusAlive &&
				phase != common.EnginePhaseRunning {

				ac := &action.PhaseChangeActionExecutor{
					Ins:         *m.ClusterSpec.SubCluster[statusEvent.ClusterID].InstanceSpec[statusEvent.Endpoint.String()],
					EnginePhase: common.EnginePhaseRunning,
					Type:        common.RwEngine,
				}
				actions = append(actions, ac)
			}
		}
	}
	return actions, nil
}

func IsNodeReady(node *v1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == v1.NodeReady {
			return c.Status == v1.ConditionTrue
		}
	}
	return false
}
