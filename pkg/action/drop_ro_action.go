package action

import (
	"fmt"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type DropRoActionExecutor struct {
	InsSpec   common.InsSpec
	RwInsSpec common.InsSpec
}

func (e *DropRoActionExecutor) String() string {
	return "DropRoActionExecutor: " + e.InsSpec.String()
}

func (e *DropRoActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	// 1. stop ro
	engineManager := resource.GetResourceManager().GetEngineManager(e.InsSpec.Endpoint)
	if err := engineManager.Stop(resource.FastStop, 2); err != nil {
		log.Warnf("Failed to fast stop, err=%s, try immediate stop", err.Error())
		if err = engineManager.Stop(resource.ImmediateStop, 5); err != nil {
			log.Warnf("Failed to immediate stop, err=%s, skip it", err.Error())

			//停ro失败
			rEvent := notify.BuildDBEventPrefixByInsV2(&e.InsSpec, common.RoEngine, e.InsSpec.Endpoint)
			rEvent.Body.Describe = fmt.Sprintf("ro instance drop: stop ro, stop type method: %s, drop err: %v", resource.ImmediateStop, err)
			rEvent.Level = notify.EventLevel_ERROR
			rEvent.EventCode = notify.EventCode_RoInstanceDrop
			sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
			if sErr != nil {
				log.Errorf("DropRoActionExecutor err: %v", sErr)
			}

		} else {
			//停ro成功
			rEvent := notify.BuildDBEventPrefixByInsV2(&e.InsSpec, common.RoEngine, e.InsSpec.Endpoint)
			rEvent.Body.Describe = fmt.Sprintf("ro instance drop: stop ro, stop type method: %s", resource.ImmediateStop)
			rEvent.Level = notify.EventLevel_WARN
			rEvent.EventCode = notify.EventCode_RoInstanceDrop
			sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
			if sErr != nil {
				log.Errorf("DropRoActionExecutor err: %v", sErr)
			}
		}
	} else {
		//停ro成功
		rEvent := notify.BuildDBEventPrefixByInsV2(&e.InsSpec, common.RoEngine, e.InsSpec.Endpoint)
		rEvent.Body.Describe = fmt.Sprintf("ro instance drop: stop ro, stop type method: %s", resource.FastStop)
		rEvent.Level = notify.EventLevel_WARN
		rEvent.EventCode = notify.EventCode_RoInstanceDrop
		sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
		if sErr != nil {
			log.Errorf("DropRoActionExecutor err: %v", sErr)
		}
	}

	// 2. drop slot
	db := resource.GetResourceManager().GetMetricsConn(e.RwInsSpec.Endpoint)
	// 1. clean old replication slot
	SlotName := resource.GenerateSlotName(common.RoEngine, &e.InsSpec)
	sql := common.InternalMarkSQL + `select pg_drop_replication_slot('` + SlotName + `')`
	_, err := db.Exec(sql)
	if err != nil {
		log.Warnf("Failed to drop slot with sql %s err %s", sql, err.Error())

		//do ro slot 失败
		rEvent := notify.BuildDBEventPrefixByInsV2(&e.InsSpec, common.RoEngine, e.InsSpec.Endpoint)
		rEvent.Body.Describe = fmt.Sprintf("ro instance drop: drop slot, slot name: %s, err: %v", SlotName, err)
		rEvent.Level = notify.EventLevel_ERROR
		rEvent.EventCode = notify.EventCode_RoInstanceSlotDrop
		sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
		if sErr != nil {
			log.Errorf("DropRoActionExecutor err: %v", sErr)
		}

		return err
	} else {
		//do ro slot 成功
		rEvent := notify.BuildDBEventPrefixByInsV2(&e.InsSpec, common.RoEngine, e.InsSpec.Endpoint)
		rEvent.Body.Describe = fmt.Sprintf("ro instance drop: drop slot, slot name: %s", SlotName)
		rEvent.Level = notify.EventLevel_INFO
		rEvent.EventCode = notify.EventCode_RoInstanceSlotDrop
		sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
		if sErr != nil {
			log.Errorf("DropRoActionExecutor err: %v", sErr)
		}
	}

	log.Infof("Success to drop slot %s", SlotName)

	return nil
}
