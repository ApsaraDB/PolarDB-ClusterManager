package action

import (
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"time"
)

type UpdatePXConfigAction struct {
	ReportError bool
}

func (*UpdatePXConfigAction) String() string {
	return "UpdatePXConfigAction"
}

func (e *UpdatePXConfigAction) Execute(m *meta.MetaManager, s *status.StatusManager) error {
	var RoSpec []common.InsSpec
	rwSpec, err := m.GetRwSpec(m.GetMasterClusterID())
	if err != nil {
		return errors.Wrapf(err, "Failed to GeneratePromoteAction")
	}

	pxConfig := ``
	for _, ins := range m.ClusterSpec.SubCluster[m.GetMasterClusterID()].InstanceSpec {
		if ins.Endpoint != rwSpec.Endpoint {
			RoSpec = append(RoSpec, *ins)
			// todo
			//pxConfig = pxConfig + `,` + cluster.Status.MasterCluster.RoHostInses[ins.ID].DbaasInstanceName + `|` + ins.Endpoint.Host + `|` + ins.Endpoint.Port
		}
	}
	pxConfig = pxConfig[1:]

	alterSQL := common.InternalMarkSQL + `alter system set polar_cluster_map = '` + pxConfig + `'`
	reloadSQL := common.InternalMarkSQL + `select pg_reload_conf()`

	log.Infof("RW: %s", rwSpec.Endpoint)

	for _, ins := range m.ClusterSpec.SubCluster[m.GetMasterClusterID()].InstanceSpec {
		retry_cnt := 0
		for {
			if retry_cnt > 1 {
				if e.ReportError && ins.Endpoint == rwSpec.Endpoint {
					/* raise error if it's RW */
					return errors.Wrapf(err, "Failed to update PX conf in %s for %d try", ins.Endpoint, retry_cnt)
				}
				log.Warnf("Failed to update PX conf in %s for %d try", ins.Endpoint, retry_cnt)
			}
			time.Sleep(time.Duration(retry_cnt*100) * time.Millisecond) // we will sleep 0ms, 100ms and quit.
			retry_cnt += 1
			log.Infof("Update PX conf in %s for %d try", ins.Endpoint, retry_cnt)

			db := resource.GetResourceManager().GetMetricsConn(ins.Endpoint)

			_, err = db.Exec(alterSQL)
			if err != nil {
				log.Warnf("Unable to set polar_cluster_map with sql: %s, %s", alterSQL, ins.Endpoint)
				continue
			}
			_, err = db.Exec(reloadSQL)
			if err != nil {
				log.Warnf("Unable to reload conf with sql: %s, %s", reloadSQL, ins.Endpoint)
				continue
			}
			break
		}
		log.Infof("Success to update PX conf in %s for %d try", ins.Endpoint, retry_cnt)
	}

	log.Infof("Success to set polar_cluster_map to %v", pxConfig)

	return nil
}
