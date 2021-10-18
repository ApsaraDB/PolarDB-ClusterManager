package decision

import (
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"time"
)

const (
	MESSAGE_INSTANCE_ALIVE  = "instance alive"
	MESSAGE_INSTANCE_LOSS   = "instance loss"
	MESSAGE_INSTANCE_DOWN   = "instance down"
	MESSAGE_INSTANCE_SWITCH = "instance switching"
	MESSAGE_VIP_ALIVE       = "vip alive"
	MESSAGE_VIP_LOSS        = "vip loss"

	FIXED_YES = "yes"
	FIXED_NO  = "no"
)

type BoxEventDecision struct {
	Ha InsAvailableDecision
}

func (d *BoxEventDecision) Name() string {
	return "BoxEventDecision"
}

func buildEngEventPrefix(ins *common.InsSpec, engineType common.EngineType, statusEvent *status.StatusEvent) map[string]interface{} {
	return map[string]interface{}{
		"endPoint":     statusEvent.Endpoint,
		"timestamp":    time.Now().UnixNano() / 1e6,
		"startAt":      time.Now().Local().Format("2006-01-02 15:04:05.000"),
		"logicInsName": resource.GetClusterManagerConfig().Cluster.Name,
		"insName":      ins.CustID,
		"podName":      ins.PodName,
		"engineType":   engineType,
	}
}

func parseEngType(m *meta.MetaManager, rwSpec *common.InsSpec, statusEvent *status.StatusEvent) common.EngineType {
	if rwSpec.Endpoint == statusEvent.Endpoint {
		if statusEvent.ClusterID == m.ClusterMeta.MasterClusterID {
			return common.RwEngine
		} else {
			return common.StandbyEngine
		}
	} else {
		return common.RoEngine
	}
}

func parseEngPhase(m *meta.MetaManager, ins *common.InsSpec, statusEvent *status.StatusEvent) string {
	subStatusMeta, sOk := m.ClusterMeta.SubCluster[statusEvent.ClusterID]
	if sOk {
		phase, pOk := subStatusMeta.EnginePhase[ins.Endpoint.String()]
		if pOk {
			return phase
		}
	}
	return ""
}

func parseIns(m *meta.MetaManager, statusEvent *status.StatusEvent) *common.InsSpec {
	subSpecMeta, ok := m.ClusterSpec.SubCluster[statusEvent.ClusterID]
	if !ok {
		return nil
	}
	return subSpecMeta.InstanceSpec[statusEvent.Endpoint.String()]

}

func (d *BoxEventDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, statusEvent *status.StatusEvent) ([]action.ActionExecutor, error) {
	if !*common.EnableAllAction {
		return d.evaluateOnDisableHA(m, s, statusEvent)
		//return nil, nil
	}
	//非alive 模式不发日志？
	//if statusEvent.CurState[common.EngineEventCategory].Name != status.EngineStatusAlive {
	//	return nil, nil
	//}

	ins := parseIns(m, statusEvent)
	if ins == nil {
		log.Errorf("Evaluate: ins is nil:%v", statusEvent.Endpoint)
		return nil, nil
	}

	rwSpec, err := m.GetRwSpec(statusEvent.ClusterID)
	if err != nil {
		log.Errorf(" %s GetRwSpec err:%v", statusEvent.Endpoint.String(), err)
		return nil, err
	}
	engineType := parseEngType(m, rwSpec, statusEvent)

	var events []map[string]interface{}
	engineEvent := buildEngEventPrefix(ins, engineType, statusEvent)

	insSpec, err := m.GetInsSpec(&statusEvent.Endpoint)
	if err != nil {
		log.Warnf("Failed to get % spec err %s", statusEvent.Endpoint.String(), err.Error())
		return nil, nil
	}
	engineReason := s.ClusterStatus.SubCluster[statusEvent.ClusterID].EngineStatus[statusEvent.Endpoint][common.EngineEventCategory].Reason
	if statusEvent.CurState[common.EngineEventCategory].Name == status.EngineStatusAlive {
		engineEvent["message"] = MESSAGE_INSTANCE_ALIVE
		engineEvent["fixed"] = FIXED_YES
		if st, exist := statusEvent.LastState[common.EngineEventCategory]; !exist || st.Name != statusEvent.CurState[common.EngineEventCategory].Name {
			events = append(events, engineEvent)
		}
	} else if available, reason := d.Ha.EvaluateInsAvailable(s.ClusterStatus.SubCluster[statusEvent.ClusterID], insSpec, engineType); !available {
		engineEvent["fixed"] = FIXED_NO
		st := s.ClusterStatus.SubCluster[statusEvent.ClusterID].EngineStatus[statusEvent.Endpoint][common.PodEngineEventCategory]
		if st.ReceivedEvents > 3 {
			engineEvent["message"] = MESSAGE_INSTANCE_DOWN
		} else {
			engineEvent["message"] = MESSAGE_INSTANCE_LOSS
		}
		if len(engineReason) != 0 {
			engineEvent["reason"] = reason
		}
		if st, exist := statusEvent.LastState[common.EngineEventCategory]; !exist || st.Name != statusEvent.CurState[common.EngineEventCategory].Name {
			events = append(events, engineEvent)
		}
	}

	vipEvent := map[string]interface{}{
		"endPoint":     statusEvent.Endpoint,
		"timestamp":    time.Now().UnixNano() / 1e6,
		"startAt":      time.Now().Local().Format("2006-01-02 15:04:05.000"),
		"logicInsName": resource.GetClusterManagerConfig().Cluster.Name,
		"insName":      ins.CustID,
		"podName":      ins.PodName,
		"engineType":   engineType,
	}
	vipReason := statusEvent.CurState[common.VipEventCategory].Reason
	if statusEvent.CurState[common.VipEventCategory].Name == status.EngineStatusAlive {
		vipEvent["message"] = MESSAGE_VIP_ALIVE
		engineEvent["fixed"] = FIXED_YES
		if st, exist := statusEvent.LastState[common.VipEventCategory]; !exist || st.Name != statusEvent.CurState[common.VipEventCategory].Name {
			events = append(events, vipEvent)
		}
	} else if statusEvent.CurState[common.VipEventCategory].Name == status.EngineStatusLosing {
		vipEvent["message"] = MESSAGE_VIP_LOSS
		engineEvent["fixed"] = FIXED_NO
		if len(vipReason) != 0 {
			vipEvent["reason"] = vipReason[len(vipReason)-1]
		}
		if st, exist := statusEvent.LastState[common.VipEventCategory]; !exist || st.Name != statusEvent.CurState[common.VipEventCategory].Name {
			events = append(events, vipEvent)
		}
	}

	var actions []action.ActionExecutor

	if len(events) != 0 {
		actions = append(actions, &action.BoxEventAction{Event: events})
	}

	return actions, nil
}

func (d *BoxEventDecision) evaluateOnDisableHA(m *meta.MetaManager, s *status.StatusManager, statusEvent *status.StatusEvent) ([]action.ActionExecutor, error) {
	ins := parseIns(m, statusEvent)
	if ins == nil {
		log.Errorf("Evaluate: ins is nil:%v", statusEvent.Endpoint)
		return nil, nil
	}

	clusterPhase := m.ClusterMeta.Phase
	engPhase := parseEngPhase(m, ins, statusEvent)

	if engPhase != common.EnginePhaseRunning || clusterPhase != common.RunningPhase {
	} else {
		return nil, nil
	}

	var actions []action.ActionExecutor
	rwSpec, err := m.GetRwSpec(statusEvent.ClusterID)
	if err != nil {
		log.Errorf(" %s GetRwSpec err:%v", statusEvent.Endpoint.String(), err)
		return nil, err
	}

	engineType := parseEngType(m, rwSpec, statusEvent)

	var events []map[string]interface{}

	engineEvent := buildEngEventPrefix(ins, engineType, statusEvent)

	insSpec, err := m.GetInsSpec(&statusEvent.Endpoint)
	if err != nil {
		log.Warnf("Failed to get % spec err %s", statusEvent.Endpoint.String(), err.Error())
		return nil, nil
	}
	engineReason := s.ClusterStatus.SubCluster[statusEvent.ClusterID].EngineStatus[statusEvent.Endpoint][common.EngineEventCategory].Reason

	if len(engineReason) > 0 {
		engineEvent["reason"] = engineReason[len(engineReason)-1]
	}
	engineEvent["message"] = MESSAGE_INSTANCE_SWITCH

	if statusEvent.CurState[common.EngineEventCategory].Name == status.EngineStatusAlive {
		engineEvent["fixed"] = FIXED_YES
	} else if available, _ := d.Ha.EvaluateInsAvailable(s.ClusterStatus.SubCluster[statusEvent.ClusterID], insSpec, common.RoEngine); !available {
		engineEvent["fixed"] = FIXED_NO
	} else {
		log.Errorf(" %s unknown status check info:%v", statusEvent.Endpoint.String(), statusEvent.CurState[common.EngineEventCategory].Name)
	}

	events = append(events, engineEvent)
	if len(events) != 0 {
		actions = append(actions, &action.BoxEventAction{Event: events})
	}

	return actions, nil
}
