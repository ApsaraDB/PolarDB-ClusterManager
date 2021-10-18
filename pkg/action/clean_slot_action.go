package action

import (
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"strings"
)

type CleanSlotActionExecutor struct {
	Ins  common.InsSpec
	Type common.EngineType
}

func (e *CleanSlotActionExecutor) String() string {
	return "CleanSlotActionExecutor: " + e.Ins.String()
}

func (e *CleanSlotActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	db := resource.GetResourceManager().GetMetricsConn(e.Ins.Endpoint)
	// 1. clean old replication slot
	type ReplicationSlot struct {
		SlotName string
	}
	var oldSlots []ReplicationSlot
	var sql string
	if e.Type == common.RoEngine {
		sql = common.InternalMarkSQL + "select slot_name from pg_replication_slots where slot_type='physical'"
	} else if e.Type == common.StandbyEngine {
		sql = common.InternalMarkSQL + "select slot_name from pg_replication_slots where slot_name like 'standby%' and slot_type='physical'"
	} else {
		return nil
	}
	_, err := db.Query(&oldSlots, sql)
	if err != nil {
		return errors.Wrapf(err, "Failed to query old slot with sql %s", sql)
	}

	rEvent := notify.BuildDBEventPrefixByInsV2(&e.Ins, common.RwEngine, e.Ins.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("Instance slot cleaned : %+v", oldSlots)
	rEvent.EventCode = notify.EventCode_InstanceSlotCleaned
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("CleanSlotActionExecutor err: %v", sErr)
	}

	for _, oldSlot := range oldSlots {
		sql := common.InternalMarkSQL + `select pg_drop_replication_slot('` + oldSlot.SlotName + `')`
		_, err := db.Exec(sql)
		if err != nil {
			if !strings.Contains(err.Error(), "is active") {
				log.Warnf("Failed to drop slot with sql %s err %s", sql, err.Error())
			}
		}
		log.Infof("Success to drop slot %s", oldSlot.SlotName)
	}
	return nil
}
