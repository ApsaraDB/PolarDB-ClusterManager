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

type UnlockReadOnlyAction struct {
}

func (*UnlockReadOnlyAction) String() string {
	return "UnlockReadOnlyAction"
}

func (e *UnlockReadOnlyAction) Execute(m *meta.MetaManager, s *status.StatusManager) error {
	rwSpec, err := m.GetRwSpec(m.GetMasterClusterID())
	if err != nil {
		return errors.Wrapf(err, "Failed to UnlockReadOnlyAction")
	}

	engineManager := resource.GetResourceManager().GetEngineManager(rwSpec.Endpoint)

	if err := engineManager.UnlockReadonly(3); err != nil {
		return errors.Wrapf(err, "Failed to UnlockReadOnlyAction")
	}

	if err := resource.GetResourceManager().GetOperatorClient().ReadOnlyLock(false); err != nil {
		if resource.IsPolarSharedMode() {
			return errors.Wrapf(err, "Failed to unlock")
		} else {
			log.Warnf("Failed to request lock err %s", err.Error())
		}
	}

	m.RemoveClusterReadOnlyTag()

	rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("Instance unlocked : %+v", rwSpec.PodName)
	rEvent.EventCode = notify.EventCode_UnLockInstanceDone
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("UnlockReadOnlyAction err: %v", sErr)
	}

	return nil
}
