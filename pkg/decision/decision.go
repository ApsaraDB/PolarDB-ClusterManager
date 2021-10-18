package decision

import (
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type StatusChangeDecision interface {
	Name() string
	Evaluate(m *meta.MetaManager, s *status.StatusManager, e *status.StatusEvent) ([]action.ActionExecutor, error)
}

type SpecChangeDecision interface {
	Name() string
	Evaluate(m *meta.MetaManager, s *status.StatusManager, e *common.SpecEvent) ([]action.ActionExecutor, error)
}
