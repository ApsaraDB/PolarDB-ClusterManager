package status

import (
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/collector"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/detector"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"strconv"
)

type StatusManager struct {
	StatusQueue             chan StatusEvent
	EngineStatuses          map[common.EndPoint]Status
	PodStatuses             map[common.EndPoint]Status
	ProxyStatuses           map[common.EndPoint]Status
	EngineDetectors         map[common.EndPoint]common.EventProducer
	CAdvisorCollectors      map[common.EndPoint]common.EventProducer
	EngineMetricsCollectors map[common.EndPoint]common.EventProducer
	PodCollectors           map[common.EndPoint]common.EventProducer
	CmCollector             map[common.EndPoint]common.EventProducer

	// 集群status, 由detector/collector等采集上了的状态，不需要持久化
	ClusterStatus *ClusterStatus
}

func NewStatusManager(statusQueue chan StatusEvent) *StatusManager {
	return &StatusManager{
		StatusQueue:             statusQueue,
		EngineStatuses:          make(map[common.EndPoint]Status),
		PodStatuses:             make(map[common.EndPoint]Status),
		ProxyStatuses:           make(map[common.EndPoint]Status),
		EngineDetectors:         make(map[common.EndPoint]common.EventProducer),
		CAdvisorCollectors:      make(map[common.EndPoint]common.EventProducer),
		EngineMetricsCollectors: make(map[common.EndPoint]common.EventProducer),
		PodCollectors:           make(map[common.EndPoint]common.EventProducer),
		CmCollector:             make(map[common.EndPoint]common.EventProducer),
		ClusterStatus:           NewClusterStatus(),
	}
}

func (m *StatusManager) Reset(statusQueue chan StatusEvent) error {
	for _, st := range m.EngineStatuses {
		err := st.Stop()
		if err != nil {
			log.Fatalf("Failed to stop %v err %s", st, err.Error())
		}
	}
	for _, st := range m.ProxyStatuses {
		err := st.Stop()
		if err != nil {
			log.Fatalf("Failed to stop %v err %s", st, err.Error())
		}
	}
	for _, st := range m.PodStatuses {
		err := st.Stop()
		if err != nil {
			log.Fatalf("Failed to stop %v err %s", st, err.Error())
		}
	}

	m.EngineStatuses = make(map[common.EndPoint]Status)
	m.PodStatuses = make(map[common.EndPoint]Status)
	m.ProxyStatuses = make(map[common.EndPoint]Status)
	m.EngineDetectors = make(map[common.EndPoint]common.EventProducer)
	m.CAdvisorCollectors = make(map[common.EndPoint]common.EventProducer)
	m.EngineMetricsCollectors = make(map[common.EndPoint]common.EventProducer)
	m.PodCollectors = make(map[common.EndPoint]common.EventProducer)
	m.CmCollector = make(map[common.EndPoint]common.EventProducer)

	m.ClusterStatus = NewClusterStatus()
	m.StatusQueue = statusQueue

	return nil
}

func (m *StatusManager) VisualString(v *meta.Visual) {
	if !resource.IsCloudOperator() && !resource.IsMpdOperator() {
		for _, cluster := range m.ClusterStatus.SubCluster {
			for endpoint, st := range cluster.EngineStatus {
				phase := "Unknown"
				if ueStat, exist := st[common.UeAgentHBEventCategory]; exist {
					if ueStat.Name == EngineStatusAlive {
						phase = "Running"
					} else if ueStat.Name == EngineStatusLosing {
						phase = "Failed"
					}
				}
				ue := meta.VisualIns{
					EndPoint: endpoint.Host + ":" + strconv.Itoa(resource.GetClusterManagerConfig().UE.Port),
					Phase:    phase,
				}
				v.Ue = append(v.Ue, ue)

				if backupStat, exist := st[common.BackupAgentHBEventCategory]; exist {
					if backupStat.Name == EngineStatusAlive {
						phase = "Running"
					} else if backupStat.Name == EngineStatusLosing {
						phase = "Failed"
					}
				}
				backupAgent := meta.VisualIns{
					EndPoint: endpoint.Host + ":" + strconv.Itoa(resource.GetClusterManagerConfig().BackupAgent.Port),
					Phase:    phase,
				}
				v.BackupAgent = append(v.BackupAgent, backupAgent)
			}
		}
	}

	for endpoint, st := range m.ClusterStatus.Proxy {
		phase := "Unknown"
		if proxyStat, exist := st[common.EngineEventCategory]; exist {
			if proxyStat.Name == ProxyStatusAlive || proxyStat.Name == ProxyStatusLosing {
				phase = "Running"
			} else if proxyStat.Name == ProxyStatusLoss ||
				proxyStat.Name == ProxyStatusFailed || proxyStat.Name == ProxyStatusDown {
				phase = "Failed"
			}
		}
		proxy := meta.VisualIns{
			EndPoint: endpoint.Host + ":" + endpoint.Port,
			Phase:    phase,
		}
		v.Proxy = append(v.Proxy, proxy)
	}
}

func (m *StatusManager) Status(meta *meta.MetaManager) string {
	return m.ClusterStatus.String(meta)
}

func (m *StatusManager) UpdateClusterStatus(statusEvent *StatusEvent) {
	if !*common.EnableAllAction {
		if st, exist := statusEvent.CurState[common.EngineEventCategory]; exist &&
			(st.Name == EngineStatusAlive || st.Name == ProxyStatusAlive) {
			log.Debugf("Pass status event %s", statusEvent.SimpleString())
		} else {
			return
		}
	}
	if statusEvent.Type == ProxyStatusType {
		if _, exist := m.ClusterStatus.Proxy[statusEvent.Endpoint]; exist {
			m.ClusterStatus.Proxy[statusEvent.Endpoint] = DeepCopyStateMap(statusEvent.CurState)
		}
	} else {
		if c, exist := m.ClusterStatus.SubCluster[statusEvent.ClusterID]; exist {
			if statusEvent.Type == EngineStatusType {
				if _, exist := c.EngineStatus[statusEvent.Endpoint]; exist {
					statusEvent.LastState = c.EngineStatus[statusEvent.Endpoint]
					c.EngineStatus[statusEvent.Endpoint] = DeepCopyStateMap(statusEvent.CurState)
					c.EngineStatusStartAt[statusEvent.Endpoint] = statusEvent.TimeStamp
				} else {
					log.Warnf("Failed to update status, endpoint %s not exist.", statusEvent.Endpoint)
					m.DeleteEngineStatus(statusEvent.Endpoint)
				}
			} else if statusEvent.Type == PodStatusType {
				if _, exist := c.PodStatus[statusEvent.Endpoint]; exist {
					statusEvent.LastState = c.PodStatus[statusEvent.Endpoint]
					c.PodStatus[statusEvent.Endpoint] = DeepCopyStateMap(statusEvent.CurState)
				} else {
					log.Warnf("Failed to update status, endpoint %s not exist.", statusEvent.Endpoint)
					m.DeletePodStatus(statusEvent.Endpoint)
				}
			}
		} else {
			log.Warnf("Failed to update status, cluster id %s not exist.", statusEvent.ClusterID)
		}
	}
}

func (m *StatusManager) DisableEngineStatus(insSpec *common.InsSpec) {
	m.EngineStatuses[insSpec.Endpoint].Disable()
}

func (m *StatusManager) EnableEngineStatus(insSpec *common.InsSpec) {
	m.EngineStatuses[insSpec.Endpoint].Enable()
}

func (m *StatusManager) DisablePodStatus(insSpec *common.InsSpec) {
	m.PodStatuses[insSpec.Endpoint].Disable()
}

func (m *StatusManager) EnablePodStatus(insSpec *common.InsSpec) {
	m.PodStatuses[insSpec.Endpoint].Enable()
}

func (m *StatusManager) CreateOrUpdateEngineStatus(clusterID string, insSpec *common.InsSpec, engineType common.EngineType, rwSpec *common.InsSpec) {
	user := resource.GetClusterManagerConfig().Account.AuroraUser
	password := resource.GetClusterManagerConfig().Account.AuroraPassword

	var eventProducers []common.EventProducer
	switch engineType {
	case common.RwEngine:
		eventProducers = append(eventProducers, detector.NewEngineDetector(clusterID, *insSpec, user, password, common.PolarDBRwHealthCheckSQL))
		break
	case common.RoEngine:
		eventProducers = append(eventProducers, detector.NewEngineDetector(clusterID, *insSpec, user, password, common.PolarDBRoHealthCheckSQL))
		break
	case common.StandbyEngine:
		if insSpec.IsDataMax {
			eventProducers = append(eventProducers, detector.NewEngineDetector(clusterID, *insSpec, user, password, common.PolarDataMaxSQL))
		} else {
			eventProducers = append(eventProducers, detector.NewEngineDetector(clusterID, *insSpec, user, password, common.PolarDBRoHealthCheckSQL))
		}
		break
	default:
		panic(engineType)
	}

	eventProducers = append(eventProducers, collector.NewEngineMetricsCollector(clusterID, insSpec, engineType, rwSpec))
	//eventProducers = append(eventProducers, collector.NewSQLMonitorCollector(insSpec, engineType, rwSpec))
	// engine conf collector
	eventProducers = append(eventProducers, collector.NewEngineConfCollector(clusterID, insSpec, engineType, rwSpec))
	// backup agent heartbeat collector
	//eventProducers = append(eventProducers, collector.NewAgentHeartbeatCollector(
	//	clusterID, insSpec, engineType, rwSpec, common.BackupAgentHBEventCategory, resource.GetClusterManagerConfig().BackupAgent.Port))
	// ue heartbeat collector
	//eventProducers = append(eventProducers, collector.NewAgentHeartbeatCollector(
	//	clusterID, insSpec, engineType, rwSpec, common.UeAgentHBEventCategory, resource.GetClusterManagerConfig().UE.Port))

	if resource.IsPolarBoxMode() {
		//eventProducers = append(eventProducers, detector.NewPodEngineDetector(*insSpec, common.PolarDBRwHealthCheckWithoutSelectSQL))
		//eventProducers = append(eventProducers, collector.NewHardwareServiceCollector(insSpec))
	}

	engineStatus := NewEngineStatus(*insSpec, eventProducers, m.StatusQueue)
	if oldEngineStatus, exist := m.EngineStatuses[insSpec.Endpoint]; exist {
		oldEngineStatus.Stop()
		go engineStatus.TriggerStatusEvent(common.EngineEventCategory)
	}
	m.EngineStatuses[insSpec.Endpoint] = engineStatus
}

func (m *StatusManager) CreateOrUpdatePodStatus(clusterID string, ins *common.InsSpec) {
	var eventProducers []common.EventProducer
	if *common.EnableCadvisorCollector {
		eventProducers = append(eventProducers, collector.NewCAdvisorCollector(clusterID, ins))
	} else if *common.EnableLogagentCollector {
		eventProducers = append(eventProducers, collector.NewLogAgentCollector(clusterID, ins))
	}
	eventProducers = append(eventProducers, collector.NewPodCollector(clusterID, ins))

	podStatus := NewPodStatus(*ins, eventProducers, m.StatusQueue)
	if oldPodStatus, exist := m.PodStatuses[ins.Endpoint]; exist {
		oldPodStatus.Stop()
	}
	m.PodStatuses[ins.Endpoint] = podStatus
}

func (m *StatusManager) DeleteEngineStatus(endpoint common.EndPoint) {
	if s, exist := m.EngineStatuses[endpoint]; exist {
		s.Stop()
		delete(m.EngineStatuses, endpoint)
	}
}

func (m *StatusManager) DeletePodStatus(endpoint common.EndPoint) {
	if s, exist := m.PodStatuses[endpoint]; exist {
		s.Stop()
		delete(m.PodStatuses, endpoint)
	}
}

func (m *StatusManager) CreateOrUpdateProxyStatus(ins *common.InsSpec, rw *common.InsSpec) {
	var eventProducers []common.EventProducer
	eventProducers = append(eventProducers, detector.NewEngineDetector(
		"",
		*ins,
		resource.GetClusterManagerConfig().Account.AuroraUser,
		resource.GetClusterManagerConfig().Account.AuroraPassword,
		common.PolarDBRoHealthCheckSQL,
	))

	// ue heartbeat collector
	if !resource.IsMpdOperator() && !resource.IsCloudOperator() {
		eventProducers = append(eventProducers, collector.NewAgentHeartbeatCollector(
			ins.ClusterID, ins, common.Proxy, rw, common.UeAgentHBEventCategory, resource.GetClusterManagerConfig().UE.Port))
	}

	proxyStatus := NewProxyStatus(*ins, eventProducers, m.StatusQueue)
	if oldProxyStatus, exist := m.ProxyStatuses[ins.Endpoint]; exist {
		oldProxyStatus.Stop()
	}
	m.ProxyStatuses[ins.Endpoint] = proxyStatus

}

func (m *StatusManager) DeleteProxyStatus(endpoint common.EndPoint) {
	if s, exist := m.ProxyStatuses[endpoint]; exist {
		s.Stop()
		delete(m.ProxyStatuses, endpoint)
	}
}

func (m *StatusManager) CreateOrUpdateCmStatus(rw *common.InsSpec) {

	// update cm status
	cmSpec := &common.InsSpec{
		Endpoint: common.EndPoint{
			Host: "127.0.0.1",
			Port: strconv.Itoa(resource.GetClusterManagerConfig().Cluster.Port),
		},
	}
	// ue heartbeat collector
	if !resource.IsMpdOperator() && !resource.IsCloudOperator() {
		cmCollector := collector.NewAgentHeartbeatCollector(
			rw.ClusterID, cmSpec, common.ClusterManager, rw, common.CmHBEventCategory, resource.GetClusterManagerConfig().UE.Port)
		if c, exist := m.CmCollector[cmSpec.Endpoint]; exist {
			c.Stop()
		}
		cmCollector.Start()

		m.CmCollector[cmSpec.Endpoint] = cmCollector
	}
}
