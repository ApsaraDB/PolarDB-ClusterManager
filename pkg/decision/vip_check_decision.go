package decision

import (
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type VipCheckDecision struct {
	meta *meta.MetaManager
}

func (d *VipCheckDecision) Name() string {
	return "VipCheckDecision"
}

func (d *VipCheckDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, statusEvent *status.StatusEvent) ([]action.ActionExecutor, error) {
	if !*common.EnableAllAction {
		return nil, nil
	}
	d.meta = m

	var actions []action.ActionExecutor
	vipM := resource.GetResourceManager().GetVipManager()

	vips := m.GetVips()

	for _, vip := range vips {
		if ep, err := vipM.GetVipEndpoint(vip.Vip); err == nil {
			if ep.String() == vip.Endpoint {

			}
		} else {

		}
	}

	return actions, nil
}
