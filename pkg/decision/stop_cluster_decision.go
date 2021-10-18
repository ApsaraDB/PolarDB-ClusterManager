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

type StopClusterDecision struct {
}

func (*StopClusterDecision) Name() string {
	return "StopClusterDecision"
}

func (d *StopClusterDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, e *common.SpecEvent) ([]action.ActionExecutor, error) {
	clusterMeta := m.ClusterMeta

	masterClusterID := m.GetMasterClusterID()

	var actions []action.ActionExecutor

	if m.GetStopped() {
		// stop cluster action
		hasRunningIns := false
		for _, subClusterStatus := range clusterMeta.SubCluster {
			for _, enginePhase := range subClusterStatus.EnginePhase {
				if enginePhase == common.EnginePhaseRunning || enginePhase == common.EnginePhaseStarting {
					hasRunningIns = true
				}
			}
		}
		if hasRunningIns {
			actions = append(actions, &action.StopClusterAction{})
			///
			rwSpec, err := m.GetRwSpec(masterClusterID)
			if err != nil {
				return nil, err
			}
			rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
			rEvent.Body.Describe = fmt.Sprintf("instance %v StopCluster decision, po: %v", rwSpec.CustID, rwSpec.PodName)
			rEvent.EventCode = notify.EventCode_StopClusterDecision
			rEvent.Level = notify.EventLevel_INFO

			sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
			if sErr != nil {
				log.Errorf("StopClusterDecision err: %v", sErr)
			}
		}
	} else {
		// start cluster action
		hasStoppedIns := false
		for _, subClusterStatus := range clusterMeta.SubCluster {
			for _, enginePhase := range subClusterStatus.EnginePhase {
				if enginePhase == common.EnginePhaseStopping || enginePhase == common.EnginePhaseStopped {
					hasStoppedIns = true
				}
			}
		}
		if hasStoppedIns {
			actions = append(actions, &action.StartClusterAction{})

			rwSpec, err := m.GetRwSpec(masterClusterID)
			if err != nil {
				return nil, err
			}
			rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
			rEvent.Body.Describe = fmt.Sprintf("instance %v StartCluster decision, po: %v", rwSpec.CustID, rwSpec.PodName)
			rEvent.EventCode = notify.EventCode_StartClusterDecision
			rEvent.Level = notify.EventLevel_INFO

			sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
			if sErr != nil {
				log.Errorf("StopClusterDecision err: %v", sErr)
			}
		}
	}

	return actions, nil
}
