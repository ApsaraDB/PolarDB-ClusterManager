package decision

import (
	"fmt"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"runtime"
	"time"
)

type InsAvailableDecision struct {
	Route       *DecisionRoute
	Reason      common.HaReason
	boxCondChan chan condEvent
}

func (t *InsAvailableDecision) EvaluateInsAvailable(s *status.SubClusterStatus, insSpec *common.InsSpec, engineType common.EngineType) (bool, common.HaReason) {
	return t.EvaluateInsAvailableV1(s, insSpec, engineType)
}

func (t *InsAvailableDecision) EvaluateInsAvailableV1(s *status.SubClusterStatus, insSpec *common.InsSpec, engineType common.EngineType) (bool, common.HaReason) {

	t.Reason.Metrics = map[string]interface{}{}
	t.Reason.DecisionTree = t.Reason.DecisionTree[:0]

	enableOnlinePromote := false
	if engineType == common.RwEngine {
		for _, st := range s.EngineStatus {
			if v, exist := st[common.EngineMetricsEventCategory].Value[common.EventOnlinePromote]; exist {
				if v.(bool) {
					enableOnlinePromote = true
				}
			}
		}
		if insSpec.ClusterType == common.PaxosCluster {
			enableOnlinePromote = true
		}
	}
	t.Reason.Metrics[common.MetricsOnlinePromote] = enableOnlinePromote

	if v, exist := s.EngineStatus[insSpec.Endpoint]; exist {
		engineState := v[common.EngineEventCategory]

		recoverySize, exist := v[common.EngineMetricsEventCategory].Value[common.EventMaxRecoverySize]
		if !exist {
			recoverySize = 0
		}
		t.Reason.Metrics[common.MetricsRecoverySize] = recoverySize

		switch engineState.Name {
		case status.EngineStatusLosing:
			// 数据库明确在启动或者恢复状态，基于恢复耗时及是否OnlinePromote决定是否切换
			if CheckReason(engineState.Reason, common.EngineRecovery) || CheckReason(engineState.Reason, common.EngineStart) {
				if enableOnlinePromote {
					// recovery的日志小于150MB, 预估恢复速度15MB/s, 预估恢复时间<10s，等待回放的代价小于切换代价,不切换
					if recoverySize.(int) < MaxOnlinePromoteRecoverySize {
						return t.EvaluateReturn(insSpec, true)
					} else {
						t.Reason.DecisionTree = append(t.Reason.DecisionTree, fmt.Sprintf("offline recovery size %d > max online promote size %d", recoverySize.(int), MaxOnlinePromoteRecoverySize))
						return t.EvaluateReturn(insSpec, false)
					}
				} else {
					return t.EvaluateReturn(insSpec, true)
				}
			} else if CheckReason(engineState.Reason, common.EngineConnRefused) {
				return t.EvaluateReturn(insSpec, false)
				// 数据库处于shutdown状态，直接切换
			} else if CheckReason(engineState.Reason, common.EngineShutdown) {
				return t.EvaluateReturn(insSpec, false)
			}

			if engineState.ReceivedEvents > 5 {
				return t.EvaluateReturn(insSpec, false)
			}
		default:
			return t.EvaluateReturn(insSpec, true)
		}
	}
	return t.EvaluateReturn(insSpec, true)
}

func (t *InsAvailableDecision) EvaluateReturn(spec *common.InsSpec, ret bool) (bool, common.HaReason) {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		if !ret {
			log.Infof("evaluate %s return %v at %s:%d", spec.String(), ret, file, line)
		}
	} else {
		log.Infof("evaluate %s return %v info miss", spec.String(), ret)
	}
	if !ret {
		t.Reason.DecisionTime = time.Since(t.Reason.RtoStart).String()
	}
	return ret, t.Reason
}
