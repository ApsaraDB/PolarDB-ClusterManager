package decision

import (
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/smartclient_service"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type SmartClientDecision struct {
	CurrentTopology *smartclient_service.TopologyEvent
}

type SmartClientSpecDecision struct {
	*SmartClientDecision
}

func (s *SmartClientSpecDecision) Name() string {
	return "SmartClientSpecDecision"
}

func (sm *SmartClientSpecDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, statusEvent *common.SpecEvent) ([]action.ActionExecutor, error) {
	return sm.SmartClientDecision.Evaluate(m)
}

type SmartClientStatusDecision struct {
	*SmartClientDecision
}

func (s *SmartClientStatusDecision) Name() string {
	return "SmartClientStatusDecision"
}

func (sm *SmartClientStatusDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, statusEvent *status.StatusEvent) ([]action.ActionExecutor, error) {
	return sm.SmartClientDecision.Evaluate(m)
}

func (sm *SmartClientDecision) Evaluate(m *meta.MetaManager) ([]action.ActionExecutor, error) {
	var actions []action.ActionExecutor
	if resource.GetResourceManager().GetSmartClientService() == nil {
		return actions, nil
	}

	topology := &smartclient_service.TopologyEvent{}
	isSame := true
	if sm.CurrentTopology == nil {
		isSame = false
	}
	for clusterID, cluster := range m.ClusterMeta.SubCluster {
		var currentTopologyNode *smartclient_service.TopologyNode
		topologyNode := &smartclient_service.TopologyNode{}
		if clusterID == m.ClusterMeta.MasterClusterID {
			topologyNode.IsMaster = true
		}

		// RW节点直接记录在cluster的RwEndpoint
		topologyNode.Rw = []*smartclient_service.DBNode{
			&smartclient_service.DBNode{
				Endpoint: cluster.RwEndpoint.String(),
			},
		}

		// 1. 找到CurrentTopology中对应的TopologyNode,并比对RW
		if sm.CurrentTopology != nil {
			for _, topNode := range sm.CurrentTopology.Topology {
				if topNode.Rw != nil && len(topNode.Rw) == 1 &&
					topNode.Rw[0].Endpoint == topologyNode.Rw[0].Endpoint {
					currentTopologyNode = topNode
					break
				}
			}
			if currentTopologyNode == nil || topologyNode.IsMaster != currentTopologyNode.IsMaster {
				isSame = false
			}
		}

		// RO节点要遍历EnginePhase，key为ip:port，value为运行状态
		for host, phrase := range cluster.EnginePhase {
			if host == cluster.RwEndpoint.String() {
				// 从所有节点中剔除RW节点
				continue
			}
			if phrase == common.EnginePhaseRunning {
				// 需要判断运行状态是否为RUNNING
				topologyNode.Ro = append(topologyNode.Ro, &smartclient_service.DBNode{
					Endpoint: host,
				})

				// 2. 比对CurrentTopology与当前拓扑的RO
				if sm.CurrentTopology != nil && isSame {
					findRo := false
					for _, roNode := range currentTopologyNode.Ro {
						if roNode.Endpoint == host {
							findRo = true
							break
						}
					}
					if !findRo {
						isSame = false
					}
				}
			}
		}
		// 3. 比对CurrentTopology与当前拓扑的len(RO)
		if sm.CurrentTopology != nil && isSame {
			isSame = (len(currentTopologyNode.Ro) == len(topologyNode.Ro))
		}

		for endpoint := range m.ProxySpec.InstanceSpec {
			// 补全Proxy
			ipport := endpoint.String()
			topologyNode.Maxscale = append(topologyNode.Maxscale, &smartclient_service.MaxScale{
				Endpoint: ipport,
			})

			// 4. 比对CurrentTopology与当前拓扑的MaxScale
			if sm.CurrentTopology != nil && isSame {
				findMaxscale := false
				for _, maxscale := range currentTopologyNode.Maxscale {
					if maxscale.Endpoint == ipport {
						findMaxscale = true
						break
					}
				}
				if !findMaxscale {
					isSame = false
				}
			}
		}
		// 5. 比对CurrentTopology与当前拓扑的len(Maxscale)
		if sm.CurrentTopology != nil && isSame {
			isSame = (len(currentTopologyNode.Maxscale) == len(topologyNode.Maxscale))
		}

		topology.Topology = append(topology.Topology, topologyNode)
	}
	// 6. 比对CurrentTopology与当前拓扑的len(Topology)
	if sm.CurrentTopology != nil && isSame {
		isSame = (len(sm.CurrentTopology.Topology) == len(topology.Topology))
	}

	if !isSame {
		sm.CurrentTopology = topology
		actions = append(actions, &action.SmartClientActionExecutor{
			Topology: sm.CurrentTopology,
		})
		log.Infof("Generate push topology desicion")
	}
	return actions, nil
}
