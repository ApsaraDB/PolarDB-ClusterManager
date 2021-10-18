package action

import (
	"fmt"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type AlarmActionExecutor struct {
	Alarm common.AlarmInfo
	Op    string
}

func (e *AlarmActionExecutor) String() string {
	return "AlarmActionExecutor: " + e.Op + " " + e.Alarm.String()
}

func (e *AlarmActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	changed := false
	if e.Op == common.AddAlarm {
		changed = metaManager.AddAlarm(e.Alarm)
	} else if e.Op == common.RemoveAlarm {
		changed = metaManager.RemoveAlarm(e.Alarm)
	} else if e.Op == common.RemoveInsAlarm {
		changed = metaManager.RemoveInsAlarm(&e.Alarm.Spec)
	} else {
		log.Errorf("AlarmActionExecutor.Execute() : %v is undefined", e.Op)
		panic(e.Op)
	}

	masterClusterID := metaManager.GetMasterClusterID()
	rwSpec, err := metaManager.GetRwSpec(masterClusterID)
	if err != nil {
		log.Errorf(" metaManager.GetRwSpec err:%v, can't build event", err)
	} else {
		insSpec := rwSpec
		if &e.Alarm.Spec != nil {
			insSpec = &e.Alarm.Spec
		}
		rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
		rEvent.Body.Describe = fmt.Sprintf("instance %v alarm action, po: %v , alarmInfo: %v", insSpec.CustID, insSpec.PodName, e.Alarm.String())

		rEvent.EventCode = notify.RedLineEventCode(fmt.Sprintf("%s:%s", notify.EventCode_CmAlarm, e.Alarm.EventType))
		if e.Op == common.AddAlarm {
			rEvent.Level = notify.EventLevel_ERROR
		} else {
			rEvent.Level = notify.EventLevel_INFO
		}

		sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
		if sErr != nil {
			log.Errorf("StorageFullDecision err: %v", sErr)
		}
	}

	if changed {
		return metaManager.SyncAlarms()
	}
	return nil
}
