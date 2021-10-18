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
)

type LockReadOnlyAction struct {
}

func (*LockReadOnlyAction) String() string {
	return "LockReadOnlyAction"
}

func (e *LockReadOnlyAction) Execute(m *meta.MetaManager, s *status.StatusManager) error {
	rwSpec, err := m.GetRwSpec(m.GetMasterClusterID())
	if err != nil {
		return errors.Wrapf(err, "Failed to lockReadOnlyAction")
	}

	engineManager := resource.GetResourceManager().GetEngineManager(rwSpec.Endpoint)

	rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("Instance locking cust: %v", rwSpec.CustID)
	rEvent.EventCode = notify.EventCode_LockingInstance
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("LockReadOnlyAction err: %v", sErr)
	}

	if err := engineManager.LockReadonly(3); err != nil {

		rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
		rEvent.Body.Describe = fmt.Sprintf("Instance locked error, cust: %v", rwSpec.CustID)
		rEvent.EventCode = notify.EventCode_LockInstanceError
		rEvent.Level = notify.EventLevel_ERROR

		sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
		if sErr != nil {
			log.Errorf("LockReadOnlyAction err: %v", sErr)
		}

		return errors.Wrapf(err, "Failed to lockReadOnlyAction")
	}

	if err := resource.GetResourceManager().GetOperatorClient().ReadOnlyLock(true); err != nil {
		if resource.IsPolarSharedMode() {
			return errors.Wrapf(err, "Failed to lock")
		} else {
			log.Warnf("Failed to request lock err %s", err.Error())
		}
	}

	m.SetClusterReadOnlyTag()

	rEvent = notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("Instance locked cust: %v", rwSpec.CustID)
	rEvent.EventCode = notify.EventCode_LockInstanceDone
	rEvent.Level = notify.EventLevel_INFO

	sErr = notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("LockReadOnlyAction err: %v", sErr)
	}

	return nil
}
