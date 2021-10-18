package decision

import (
	"fmt"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type StorageFullDecision struct {
	LastSuccessReportState string
}

func (d *StorageFullDecision) Name() string {
	return "StorageFullDecision"
}

func (d *StorageFullDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, statusEvent *status.StatusEvent) ([]action.ActionExecutor, error) {
	if statusEvent.Type == status.ProxyStatusType {
		return nil, nil
	}
	if !*common.EnableAllAction && statusEvent.CurState[common.EngineEventCategory].Name != status.EngineStatusAlive {
		return nil, nil
	}
	// TODO: all need
	if _, exist := statusEvent.CurState[common.StorageUsageEventCategory]; !exist {
		return nil, nil
	}
	rw, err := m.GetRwSpec(m.GetMasterClusterID())
	if err != nil {
		return nil, nil
	}
	if statusEvent.Endpoint != rw.Endpoint {
		return nil, nil
	}

	var actions []action.ActionExecutor

	if statusEvent.CurState[common.StorageUsageEventCategory].Name == status.PodStatusStorageFull &&
		d.LastSuccessReportState != status.PodStatusStorageFull {

		d.LastSuccessReportState = status.PodStatusStorageFull

		rEvent := notify.BuildDBEventPrefixByInsV2(rw, common.RwEngine, rw.Endpoint)
		rEvent.Body.Describe = fmt.Sprintf("instance %v storage full decision, po: %v", rw.CustID, rw.PodName)
		rEvent.EventCode = notify.EventCode_InstanceStorageFull
		rEvent.Level = notify.EventLevel_ERROR

		sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
		if sErr != nil {
			log.Errorf("StorageFullDecision err: %v", sErr)
		}

		ac := &action.StorageFullActionExecutor{
			Lock: true,
		}
		actions = append(actions, ac)
		return actions, nil
	} else if statusEvent.CurState[common.StorageUsageEventCategory].Name == status.PodStatusStorageNormal &&
		d.LastSuccessReportState != statusEvent.CurState[common.StorageUsageEventCategory].Name {

		d.LastSuccessReportState = statusEvent.CurState[common.StorageUsageEventCategory].Name

		rEvent := notify.BuildDBEventPrefixByInsV2(rw, common.RwEngine, rw.Endpoint)
		rEvent.Body.Describe = fmt.Sprintf("instance %v storage not full decision, po: %v", rw.CustID, rw.PodName)
		rEvent.EventCode = notify.EventCode_InstanceStorageNotFull
		rEvent.Level = notify.EventLevel_INFO

		sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
		if sErr != nil {
			log.Errorf("StorageFullDecision err: %v", sErr)
		}

		ac := &action.StorageFullActionExecutor{
			Lock: false,
		}
		actions = append(actions, ac)
		return actions, nil
	}

	return nil, nil
}
