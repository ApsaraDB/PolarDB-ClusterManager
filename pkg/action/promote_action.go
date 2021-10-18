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

type PromoteActionExecutor struct {
	Ins   common.InsSpec
	RoIns []common.InsSpec
}

func (e *PromoteActionExecutor) String() string {
	return "PromoteActionExecutor: " + e.Ins.String()
}

func (e *PromoteActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {

	rEvent := notify.BuildDBEventPrefixByInsV2(&e.Ins, common.RwEngine, e.Ins.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("instance %v promote task create, po: %v", e.Ins.CustID, e.Ins.PodName)
	rEvent.EventCode = notify.EventCode_PormotionTaskCreated
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("PromoteActionExecutor err: %v", sErr)
	}

	engineManager := resource.GetResourceManager().GetEngineManager(e.Ins.Endpoint)

	statusManager.DisableEngineStatus(&e.Ins)

	engineManager.Promote(common.RwEngine, e.RoIns, false)

	metaManager.SetInsPhase(&e.Ins, common.EnginePhaseStarting)

	statusManager.EnableEngineStatus(&e.Ins)

	ExecuteWithRetry(metaManager.Sync, 10, true)

	log.Infof("Success to promote %s", e.Ins.String())

	return nil
}
