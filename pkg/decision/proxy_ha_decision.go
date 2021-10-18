package decision

import (
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"time"
)

type ProxyHaDecision struct {
}

func (d *ProxyHaDecision) Name() string {
	return "ProxyHaDecision"
}

func (d *ProxyHaDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, statusEvent *status.StatusEvent) ([]action.ActionExecutor, error) {
	proxySpec := m.ProxySpec
	clusterMeta := m.ClusterMeta
	clusterStatus := s.ClusterStatus

	var actions []action.ActionExecutor

	if statusEvent.Type != status.ProxyStatusType {
		return actions, nil
	}
	if !*common.EnableAllAction && statusEvent.CurState[common.EngineEventCategory].Name != status.EngineStatusAlive {
		return actions, nil
	}

	if statusEvent.CurState[common.EngineEventCategory].Name == status.ProxyStatusAlive {
		ac := &action.ProxyReportActionExecutor{
			Ins:     *proxySpec.InstanceSpec[statusEvent.Endpoint],
			Healthy: true,
		}
		actions = append(actions, ac)
		return actions, nil
	} else if statusEvent.CurState[common.EngineEventCategory].Name == status.ProxyStatusDown {
		st := clusterStatus.SubCluster[clusterMeta.MasterClusterID].EngineStatus[clusterMeta.SubCluster[clusterMeta.MasterClusterID].RwEndpoint]
		rwPhase, rwPhaseStartAt := m.GetRwEnginePhase()
		if st[common.EngineEventCategory].Name == status.EngineStatusAlive &&
			rwPhase == common.EnginePhaseRunning &&
			time.Since(rwPhaseStartAt).Seconds() > 10 {
			ac := &action.ProxyReportActionExecutor{
				Ins:     *proxySpec.InstanceSpec[statusEvent.Endpoint],
				Healthy: false,
			}
			actions = append(actions, ac)
			return actions, nil
		} else {
			log.Infof("Proxy %s down but rw is in %s phase since %s", statusEvent.Endpoint.String(), rwPhase, rwPhaseStartAt.String())
		}
	}

	return actions, nil
}
