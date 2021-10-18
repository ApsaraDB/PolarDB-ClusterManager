package decision

import (
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/smartclient_service"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type TopologyChangeDecision struct {
	CurrentTopology *smartclient_service.TopologyEvent
}

func (s *TopologyChangeDecision) Name() string {
	return "TopologyChangeDecision"
}
func (sm *TopologyChangeDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, statusEvent *common.SpecEvent) ([]action.ActionExecutor, error) {
	var actions []action.ActionExecutor

	topology := &smartclient_service.TopologyEvent{}
	isSame := true
	if sm.CurrentTopology == nil {
		isSame = false
	}
	var datamaxClusterID string
	for _, cluster := range m.ClusterSpec.SubCluster {
		for _, ins := range cluster.InstanceSpec {
			if ins.IsDataMax {
				datamaxClusterID = ins.ClusterID
			}
		}
	}
	for clusterID, cluster := range m.ClusterMeta.SubCluster {
		var currentTopologyNode *smartclient_service.TopologyNode
		topologyNode := &smartclient_service.TopologyNode{}
		if clusterID == m.ClusterMeta.MasterClusterID {
			topologyNode.IsMaster = true
		}
		if clusterID == datamaxClusterID {
			topologyNode.IsDataMax = true
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
		actions = append(actions, &action.TopologyChangeAction{
			Topology: sm.CurrentTopology,
		})
	}
	return actions, nil
}
