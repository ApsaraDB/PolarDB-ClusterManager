package action

import (
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/smartclient_service"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type SmartClientActionExecutor struct {
	Topology *smartclient_service.TopologyEvent
}

func (s *SmartClientActionExecutor) String() string {
	return "SmartClientActionExecutor"
}

func (s *SmartClientActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	resource.GetResourceManager().GetSmartClientService().Send(&smartclient_service.Response{TopologyEvent: s.Topology})
	log.Infof("Push topology to client: %s", s.Topology.String())
	return nil
}
