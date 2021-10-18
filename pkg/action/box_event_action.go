package action

import (
	"fmt"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"strings"
)

type BoxEventAction struct {
	Event []map[string]interface{}
}

func (e *BoxEventAction) String() string {
	return "BoxEventAction"
}

func (e *BoxEventAction) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	masterClusterID := metaManager.GetMasterClusterID()
	rwSpec, err := metaManager.GetRwSpec(masterClusterID)
	if err != nil {
		log.Errorf("get rwSpec err: %v", err)
		return err
	}
	for _, ev := range e.Event {
		if *common.EnableRedLine {
			rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
			rEvent.EventCode = parseEventCode(fmt.Sprintf("%s", ev["message"]))
			ev["message"] = fmt.Sprintf("[StateMachine]%s", ev["message"])
			rEvent.Body.Describe = evToString(ev)

			rEvent.Level = notify.EventLevel_WARN

			sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
			if sErr != nil {
				log.Errorf("_BoxEventAction err: %v", sErr)
				return sErr
			}
		} else {
			err := resource.GetResourceManager().GetEsClient().SendBoxInsEvent(ev)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func evToString(ev map[string]interface{}) string {
	if len(ev) < 1 {
		return ""
	}
	var bf []string
	for k, v := range ev {
		bf = append(bf, fmt.Sprintf("%s:%v", k, v))
	}
	return strings.Join(bf, ", ")
}

func parseEventCode(oldMsg string) notify.RedLineEventCode {
	if oldMsg == "" {
		return notify.EventCode_UNKNOWN
	}
	if oldMsg == string(notify.EventCode_MESSAGE_INSTANCE_ALIVE) {
		return notify.EventCode_MESSAGE_INSTANCE_ALIVE
	} else if oldMsg == string(notify.EventCode_MESSAGE_INSTANCE_LOSS) {
		return notify.EventCode_MESSAGE_INSTANCE_LOSS
	} else if oldMsg == string(notify.EventCode_MESSAGE_INSTANCE_DOWN) {
		return notify.EventCode_MESSAGE_INSTANCE_DOWN
	} else if oldMsg == string(notify.EventCode_MESSAGE_INSTANCE_SWITCH) {
		return notify.EventCode_MESSAGE_INSTANCE_SWITCH
	} else if oldMsg == string(notify.EventCode_MESSAGE_VIP_ALIVE) {
		return notify.EventCode_MESSAGE_VIP_ALIVE
	} else if oldMsg == string(notify.EventCode_MESSAGE_VIP_LOSS) {
		return notify.EventCode_MESSAGE_VIP_LOSS
	}
	newMsg := fmt.Sprintf("%v:%s", notify.EventCode_Others, oldMsg)
	return notify.RedLineEventCode(newMsg)
}
