package action

import (
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type ActionType string

type ActionExecutor interface {
	String() string
	Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error
}

const (
	Switchover    ActionType = "Switchover"
	NotSwitchOver ActionType = "NotSwitchover"
)

func GenerateRestartAction(ins *common.InsSpec, engineType common.EngineType) (ActionExecutor, error) {
	log.Infof("generate restart action %s", ins.String())
	return &RestartActionExecutor{
		Ins:        *ins,
		EngineType: engineType,
	}, nil

}

func GeneratePromoteAction(m *meta.MetaManager) (ActionExecutor, error) {
	var RoSpec []common.InsSpec
	rwSpec, err := m.GetRwSpec(m.GetMasterClusterID())
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to GeneratePromoteAction")
	}
	for _, ins := range m.ClusterSpec.SubCluster[m.GetMasterClusterID()].InstanceSpec {
		if ins.Endpoint != rwSpec.Endpoint {
			RoSpec = append(RoSpec, *ins)
		}
	}
	return &PromoteActionExecutor{
		Ins:   *rwSpec,
		RoIns: RoSpec,
	}, nil
}

func GenerateStandbySwitchAction(
	m *meta.MetaManager, s *status.StatusManager, reason common.HaReason, manual, force bool,
	fromClusterID, toClusterID string, rpo, rto int) (ActionExecutor, error) {
	log.Infof("generate standby switch action manual:%v force:%v from:%s to:%s", manual, force, fromClusterID, toClusterID)
	clusterSpec := m.ClusterSpec
	clusterStatus := s.ClusterStatus

	var newRwSpec *common.InsSpec
	var oldRwSpec *common.InsSpec
	var oldStandbySpec []common.InsSpec
	var err error

	for _, subCluster := range clusterSpec.SubCluster {
		if subCluster.ClusterID == fromClusterID {
			if oldRwSpec, err = m.GetRwSpec(subCluster.ClusterID); err != nil {
				return nil, err
			}
		} else if subCluster.ClusterID == toClusterID {
			if newRwSpec, err = m.GetRwSpec(subCluster.ClusterID); err != nil {
				return nil, err
			}
		} else {
			if standbySpec, err := m.GetRwSpec(subCluster.ClusterID); err != nil {
				return nil, err
			} else {
				if standbySpec.IsDeleted {
					log.Debugf("Skip deleted standby %s", standbySpec.String())
					continue
				}
				oldStandbySpec = append(oldStandbySpec, *standbySpec)
			}
		}
	}

	recoverySize, exist := clusterStatus.SubCluster[oldRwSpec.ClusterID].EngineStatus[oldRwSpec.Endpoint][common.EngineMetricsEventCategory].Value[common.EventMaxRecoverySize]
	if !exist {
		recoverySize = 0
	}
	if newRwSpec.ClusterType == common.PaxosCluster {
		return &LeaderFollowerSwitchActionExecutor{
			SwitchActionExecutor: SwitchActionExecutor{
				SwitchAction: common.SwitchAction{
					OldRw:        *oldRwSpec,
					NewRw:        *newRwSpec,
					OldStandby:   oldStandbySpec,
					Manual:       manual,
					Force:        force,
					Type:         common.MasterStandbySwitch,
					RecoverySize: int64(recoverySize.(int)),
					RTO:          rto,
				},
				Event: common.SwitchEvent{
					Reason: reason,
				},
			},
		}, nil
	} else {
		return &MasterStandbySwitchActionExecutor{
			SwitchActionExecutor: SwitchActionExecutor{
				SwitchAction: common.SwitchAction{
					OldRw:        *oldRwSpec,
					NewRw:        *newRwSpec,
					OldStandby:   oldStandbySpec,
					Manual:       manual,
					Force:        force,
					RPO:          rpo,
					RTO:          rto,
					Type:         common.MasterStandbySwitch,
					RecoverySize: int64(recoverySize.(int)),
				},
				Event: common.SwitchEvent{
					Reason: reason,
				},
			},
		}, nil
	}
}

func GenerateReplicaSwitchAction(
	m *meta.MetaManager,
	s *status.StatusManager,
	reason common.HaReason,
	manual, force bool,
	clusterID string,
	newRw *common.EndPoint) (ActionExecutor, error) {
	log.Infof("generate replica switch action manual:%v force:%v cluster:%s", manual, force, clusterID)
	if newRw != nil {
		var oldRoSpec []common.InsSpec
		var newRwSpec common.InsSpec
		var oldRwSpec common.InsSpec
		for _, ins := range m.ClusterSpec.SubCluster[clusterID].InstanceSpec {
			if ins.Endpoint != *newRw {
				if ins.IsDeleted {
					log.Debugf("Skip deleted ro %s", ins.String())
					continue
				}
				if ins.Endpoint == m.ClusterMeta.SubCluster[clusterID].RwEndpoint {
					oldRwSpec = *ins
				} else {
					oldRoSpec = append(oldRoSpec, *ins)
				}
			} else {
				newRwSpec = *ins
			}
		}
		var oldStandbySpec []common.InsSpec
		for id, cluster := range m.ClusterSpec.SubCluster {
			if id != clusterID {
				standby := cluster.InstanceSpec[m.ClusterMeta.SubCluster[id].RwEndpoint.String()]
				if standby.IsDeleted {
					log.Debugf("Skip deleted standby %s", standby.String())
					continue
				}
				oldStandbySpec = append(oldStandbySpec, *standby)
			}
		}

		recoverySize, exist := s.ClusterStatus.SubCluster[oldRwSpec.ClusterID].EngineStatus[oldRwSpec.Endpoint][common.EngineMetricsEventCategory].Value[common.EventMaxRecoverySize]
		if !exist {
			recoverySize = 0
		}
		if clusterID == m.ClusterMeta.MasterClusterID {
			return &MasterReplicaSwitchActionExecutor{
				SwitchActionExecutor: SwitchActionExecutor{
					SwitchAction: common.SwitchAction{
						OldRw:        oldRwSpec,
						OldRo:        oldRoSpec,
						NewRw:        newRwSpec,
						OldStandby:   oldStandbySpec,
						Manual:       manual,
						Force:        force,
						Type:         common.MasterReplicaSwitch,
						RecoverySize: int64(recoverySize.(int)),
					},
					Event: common.SwitchEvent{
						Reason: reason,
					},
				},
			}, nil
		} else {
			masterSpec, err := m.GetRwSpec(m.GetMasterClusterID())
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to GenerateReplicaSwitchAction")
			}
			return &StandbyReplicaSwitchActionExecutor{
				SwitchActionExecutor: SwitchActionExecutor{
					SwitchAction: common.SwitchAction{
						OldRw:        oldRwSpec,
						OldRo:        oldRoSpec,
						NewRw:        newRwSpec,
						Manual:       force,
						Force:        force,
						Master:       *masterSpec,
						Type:         common.StandbyReplicaSwitch,
						RecoverySize: int64(recoverySize.(int)),
					},
					Event: common.SwitchEvent{
						Reason: reason,
					},
				},
			}, nil
		}
	} else {
		return nil, errors.New("Invalid new rw")
	}
}

func NewSwitchActionExecutor(action common.SwitchAction, reason common.HaReason) ActionExecutor {
	if action.Type == common.MasterReplicaSwitch {
		return &MasterReplicaSwitchActionExecutor{
			SwitchActionExecutor{SwitchAction: action, Event: common.SwitchEvent{
				Reason: reason,
			}}}
	} else if action.Type == common.MasterStandbySwitch {
		return &MasterStandbySwitchActionExecutor{
			SwitchActionExecutor{SwitchAction: action, Event: common.SwitchEvent{
				Reason: reason,
			}}}
	} else if action.Type == common.StandbyReplicaSwitch {
		return &StandbyReplicaSwitchActionExecutor{
			SwitchActionExecutor{SwitchAction: action, Event: common.SwitchEvent{
				Reason: reason,
			}}}
	} else {
		return nil
	}
}

func GenerateUpdatePXConfigAction(reportError bool) ActionExecutor {
	log.Infof("generate UpdatePXConfig action")
	return &UpdatePXConfigAction{ReportError: reportError}
}
