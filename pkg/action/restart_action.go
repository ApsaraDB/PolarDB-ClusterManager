package action

import (
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type RestartActionExecutor struct {
	Ins        common.InsSpec
	EngineType common.EngineType
}

func (e *RestartActionExecutor) String() string {
	return "RestartActionExecutor: " + e.Ins.String()
}

func (e *RestartActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	engineManager := resource.GetResourceManager().GetEngineManager(e.Ins.Endpoint)

	if phase, _, err := metaManager.GetInsPhase(&e.Ins); err != nil {
		return err
	} else if phase == common.EnginePhaseStarting {
		log.Infof("Skip restart action cause ins %s in restarting", e.Ins.String())
		return nil
	}

	statusManager.DisableEngineStatus(&e.Ins)
	defer statusManager.EnableEngineStatus(&e.Ins)

	if err := engineManager.Stop(resource.FastStop, 2); err != nil {
		log.Warnf("Failed to fast stop, err=%s, try immediate stop", err.Error())
		if err = engineManager.Stop(resource.ImmediateStop, 2); err != nil {
			log.Warnf("Failed to immediate stop, err=%s, skip it", err.Error())
			return err
		}
	}
	if e.EngineType == common.RwEngine {
		if err := engineManager.Promote(common.RwEngine, nil, false); err != nil {
			log.Warnf("Failed to start, err=%s, skip it", err.Error())
			return err
		}
	} else {
		return errors.Errorf("not support reastart %s type %s", e.Ins.String(), e.EngineType)
	}

	metaManager.SetInsPhase(&e.Ins, common.EnginePhaseStarting)

	ExecuteWithRetry(metaManager.Sync, 10, true)

	log.Infof("Success to restart %s", e.Ins.String())

	return nil
}
