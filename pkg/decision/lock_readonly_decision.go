package decision

import (
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type LockReadOnlyDecision struct {
}

func (*LockReadOnlyDecision) Name() string {
	return "LockReadOnlyDecision"
}

func (d *LockReadOnlyDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, e *common.SpecEvent) ([]action.ActionExecutor, error) {
	var actions []action.ActionExecutor

	if m.GetSpecLockReadOnly() {
		if !m.GetClusterReadOnly() {
			if phase, _ := m.GetRwEnginePhase(); phase == common.EnginePhaseRunning {
				actions = append(actions, &action.LockReadOnlyAction{})
			} else {
				log.Infof("Rw phase %s could not lock", phase)
			}
		}
	} else {
		if m.GetClusterReadOnly() {
			if phase, _ := m.GetRwEnginePhase(); phase == common.EnginePhaseRunning {
				actions = append(actions, &action.UnlockReadOnlyAction{})
			} else {
				log.Infof("Rw phase %s could not unlock", phase)
			}
		}
	}

	return actions, nil
}
