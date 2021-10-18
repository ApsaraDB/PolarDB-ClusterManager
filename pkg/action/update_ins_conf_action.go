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

type UpdateInsConfActionExecutor struct {
	Ins      common.InsSpec
	Settings []common.FileSetting
}

func (e *UpdateInsConfActionExecutor) String() string {
	return "UpdateInsConfActionExecutor: " + e.Ins.String()
}

func (e *UpdateInsConfActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	engineManager := resource.GetResourceManager().GetEngineManager(e.Ins.Endpoint)

	rEvent := notify.BuildDBEventPrefixByInsV2(&e.Ins, common.RwEngine, e.Ins.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("Instance update config, pod: %+v", e.Ins.PodName)
	rEvent.EventCode = notify.EventCode_UpateaInstanceConfig
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("UpdateInsConfActionExecutor err: %v", sErr)
	}
	err := engineManager.UpdateConf(e.Settings)
	if err != nil {
		log.Warnf("Failed to update conf err=%s", err.Error())
		return err
	}

	log.Infof("Success to update %s conf with conf %v", e.Ins.String(), e.Settings)

	return nil
}
