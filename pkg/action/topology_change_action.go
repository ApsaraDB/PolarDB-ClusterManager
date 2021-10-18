package action

import (
	"fmt"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/plugin"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/smartclient_service"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type TopologyChangeAction struct {
	Topology *smartclient_service.TopologyEvent
}

func (a *TopologyChangeAction) String() string {
	return "TopologyChangeAction"
}

func (a *TopologyChangeAction) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {

	masterClusterID := metaManager.GetMasterClusterID()
	rwSpec, err := metaManager.GetRwSpec(masterClusterID)

	if err != nil {
		log.Errorf(" metaManager.GetRwSpec err:%v, can't build event", err)
	} else {
		rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
		rEvent.Body.Describe = fmt.Sprintf("instance %v topology changed, po: %v", rwSpec.CustID, rwSpec.PodName)
		rEvent.EventCode = notify.EventCode_DbTopologyChange
		rEvent.Level = notify.EventLevel_INFO

		sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
		if sErr != nil {
			log.Errorf("StorageFullDecision err: %v", sErr)
		}
	}

	pluginManager := plugin.GetPluginManager(nil)

	param := make(map[string]interface{})

	param[common.PLUGIN_TOPOLOGY_EVENT] = *a.Topology

	pluginManager.Run(param)

	return nil
}
