package decision

import (
	"encoding/json"
	"github.com/prometheus/common/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"io/ioutil"
	"sync"
)

var once sync.Once
var route *DecisionRoute

func GetDecisionRoute() *DecisionRoute {
	once.Do(func() {
		log.Infof("Loading decision route")
		r := DecisionRoute{}
		v, err := meta.GetMetaManager().GetCmConf(HaDecisionRoute)
		if err != nil {
			log.Infof("Load ha policy from conf")

			path := *common.WorkDir + "/" + *common.HaPolicyConfPath
			data, err := ioutil.ReadFile(path)
			if err == nil {
				err = json.Unmarshal(data, &r)
				if err != nil {
					log.Warnf("Failed to parse ha policy config json %s err %s", string(data), err.Error())
				} else {
					route = &r
					return
				}
			} else {
				log.Warnf("Failed to load ha policy config json from %s err %s", path, err.Error())
			}

			log.Infof("Load ha policy from default")

			route = NewHaDefaultDecisionRoute()
		} else {
			err = json.Unmarshal([]byte(v.(string)), &r)
			if err != nil {
				log.Warnf("Failed to unmarshal ha decision route %s err %s", v.(string), err.Error())
				route = NewHaDefaultDecisionRoute()
			} else {
				log.Infof("Load ha policy from meta")
				route = &r
			}
		}
	})
	return route
}

func NewHaDefaultDecisionRoute() *DecisionRoute {

	route := &DecisionRoute{
		Paths: map[int]DecisionPath{},
	}
	route.AddDecisionPath(
		DecisionPath{
			Nodes: []DecisionPredicator{
				{
					Key:   common.MetricsEngineState,
					Value: status.EngineStatusLosing,
					Op:    PredicatorEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsDetectError,
					Value: common.EngineRecovery,
					Op:    PredicatorInOp,
					Type:  PredicatorKeyArrayType,
				},
				{
					Key:   common.MetricsOnlinePromote,
					Value: true,
					Op:    PredicatorEqualOp,
					Type:  PredicatorBoolType,
				},
				{
					Key:   common.MetricsRecoverySize,
					Value: MaxOnlinePromoteRecoverySize,
					Op:    PredicatorLargeOp,
					Type:  PredicatorIntType,
				},
				{
					Key:   common.MetricsDecisionTime,
					Value: 0,
					Op:    PredicatorLargeOp,
					Type:  PredicatorIntType,
				},
			},
		},
	)
	route.AddDecisionPath(
		DecisionPath{
			Nodes: []DecisionPredicator{
				{
					Key:   common.MetricsEngineState,
					Value: status.EngineStatusLosing,
					Op:    PredicatorEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsDetectError,
					Value: common.EngineStart,
					Op:    PredicatorInOp,
					Type:  PredicatorKeyArrayType,
				},
				{
					Key:   common.MetricsOnlinePromote,
					Value: true,
					Op:    PredicatorEqualOp,
					Type:  PredicatorBoolType,
				},
				{
					Key:   common.MetricsRecoverySize,
					Value: MaxOnlinePromoteRecoverySize,
					Op:    PredicatorLargeOp,
					Type:  PredicatorIntType,
				},
				{
					Key:   common.MetricsDecisionTime,
					Value: 0,
					Op:    PredicatorLargeOp,
					Type:  PredicatorIntType,
				},
			},
		},
	)

	route.AddDecisionPath(
		DecisionPath{
			Nodes: []DecisionPredicator{
				{
					Key:   common.MetricsEngineState,
					Value: status.EngineStatusLosing,
					Op:    PredicatorEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsDetectError,
					Value: common.EngineConnRefused,
					Op:    PredicatorInOp,
					Type:  PredicatorKeyArrayType,
				},
				{
					Key:   common.MetricsDecisionTime,
					Value: 0,
					Op:    PredicatorLargeOp,
					Type:  PredicatorIntType,
				},
			},
		},
	)

	route.AddDecisionPath(
		DecisionPath{
			Nodes: []DecisionPredicator{
				{
					Key:   common.MetricsEngineState,
					Value: status.EngineStatusLosing,
					Op:    PredicatorEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsDetectError,
					Value: common.EngineShutdown,
					Op:    PredicatorInOp,
					Type:  PredicatorKeyArrayType,
				},
				{
					Key:   common.MetricsDecisionTime,
					Value: 0,
					Op:    PredicatorLargeOp,
					Type:  PredicatorIntType,
				},
			},
		},
	)

	route.AddDecisionPath(
		DecisionPath{
			Nodes: []DecisionPredicator{
				{
					Key:   common.MetricsEngineState,
					Value: status.EngineStatusLosing,
					Op:    PredicatorEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsDetectError,
					Value: common.EngineReadOnly,
					Op:    PredicatorInOp,
					Type:  PredicatorKeyArrayType,
				},
				{
					Key:   common.MetricsPaxosRole,
					Value: 2,
					Op:    PredicatorNotEqualOp,
					Type:  PredicatorIntType,
				},
				{
					Key:   common.MetricsDecisionTime,
					Value: 5,
					Op:    PredicatorLargeOp,
					Type:  PredicatorIntType,
				},
				{
					Key:   common.MetricsDecisionTime,
					Value: 0,
					Op:    PredicatorLargeOp,
					Type:  PredicatorIntType,
				},
			},
		},
	)

	route.AddDecisionPath(
		DecisionPath{
			Nodes: []DecisionPredicator{
				{
					Key:   common.MetricsEngineState,
					Value: status.EngineStatusLosing,
					Op:    PredicatorEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsDetectError,
					Value: common.EngineTimeout,
					Op:    PredicatorInOp,
					Type:  PredicatorKeyArrayType,
				},
				{
					Key:   common.MetricsWaitEvent,
					Value: status.EngineMetricsWait,
					Op:    PredicatorNotEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsCpuStatus,
					Value: status.PodStatusTimeout,
					Op:    PredicatorEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsDecisionTime,
					Value: 10,
					Op:    PredicatorLargeOp,
					Type:  PredicatorIntType,
				},
			},
		},
	)

	route.AddDecisionPath(
		DecisionPath{
			Nodes: []DecisionPredicator{
				{
					Key:   common.MetricsEngineState,
					Value: status.EngineStatusLosing,
					Op:    PredicatorEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsDetectError,
					Value: common.EngineTimeout,
					Op:    PredicatorInOp,
					Type:  PredicatorKeyArrayType,
				},
				{
					Key:   common.MetricsWaitEvent,
					Value: status.EngineMetricsWait,
					Op:    PredicatorNotEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsCpuStatus,
					Value: status.PodStatusUnknown,
					Op:    PredicatorEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsDecisionTime,
					Value: 15,
					Op:    PredicatorLargeOp,
					Type:  PredicatorIntType,
				},
			},
		},
	)

	route.AddDecisionPath(
		DecisionPath{
			Nodes: []DecisionPredicator{
				{
					Key:   common.MetricsEngineState,
					Value: status.EngineStatusLosing,
					Op:    PredicatorEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsDetectError,
					Value: common.EngineTimeout,
					Op:    PredicatorInOp,
					Type:  PredicatorKeyArrayType,
				},
				{
					Key:   common.MetricsWaitEvent,
					Value: status.EngineMetricsWait,
					Op:    PredicatorNotEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsCpuStatus,
					Value: status.PodStatusCpuEmpty,
					Op:    PredicatorEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsOnlinePromote,
					Value: true,
					Op:    PredicatorEqualOp,
					Type:  PredicatorBoolType,
				},
				{
					Key:   common.MetricsDecisionTime,
					Value: 15,
					Op:    PredicatorLargeOp,
					Type:  PredicatorIntType,
				},
			},
		},
	)

	route.AddDecisionPath(
		DecisionPath{
			Nodes: []DecisionPredicator{
				{
					Key:   common.MetricsEngineState,
					Value: status.EngineStatusLosing,
					Op:    PredicatorEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsCpuStatus,
					Value: status.PodStatusCpuFull,
					Op:    PredicatorNotEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsWaitEvent,
					Value: status.EngineMetricsWait,
					Op:    PredicatorNotEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsDecisionTime,
					Value: 3600,
					Op:    PredicatorLargeOp,
					Type:  PredicatorIntType,
				},
			},
		},
	)

	route.AddDecisionPath(
		DecisionPath{
			Nodes: []DecisionPredicator{
				{
					Key:   common.MetricsEngineState,
					Value: status.EngineStatusLosing,
					Op:    PredicatorEqualOp,
					Type:  PredicatorStringType,
				},
				{
					Key:   common.MetricsDetectError,
					Value: common.EngineRecovery,
					Op:    PredicatorInOp,
					Type:  PredicatorKeyArrayType,
				},
				{
					Key:   common.MetricsDecisionTime,
					Value: 3600,
					Op:    PredicatorLargeOp,
					Type:  PredicatorIntType,
				},
			},
		},
	)

	return route
}
