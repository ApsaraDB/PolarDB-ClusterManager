package meta

import (
	"context"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"k8s.io/client-go/tools/leaderelection"
	"strconv"
	"sync"
)

type ConsensusService interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
	Run(enableRecovery bool, ctx context.Context, lec leaderelection.LeaderElectionConfig) error
	Join(nodeID, addr string) error
	Remove(nodeID, addr string) error
	Status() string
	MemberStatus() map[string]string
	LeaderTransfer(nodeID, addr string) error
}

var onceConsensus sync.Once
var cs ConsensusService

func GetConsensusService() ConsensusService {
	onceConsensus.Do(func() {
		if resource.IsPolarPureMode() {
			if resource.IsEtcdStorage() {
				cs = NewK8sConsensusService()
			} else {
				raft := NewRaftConsensusService()
				raft.RaftBind = resource.GetLocalServerAddr(false) + ":" + strconv.Itoa(resource.GetClusterManagerConfig().Consensus.Port)
				raft.RaftDir = *common.WorkDir + "/" + *common.ConsensusPath
				cs = raft
			}
		} else {
			cs = NewK8sConsensusService()
		}
	})
	return cs
}
