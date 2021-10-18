package decision

import (
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/detector"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"k8s.io/api/core/v1"
	"strconv"
	"time"
)

const (
	MaxOnlinePromoteRecoverySize  = 150 * 1024 * 1024
	MaxOfflinePromoteRecoverySize = 1024 * 1024 * 1024
	RecoverySpeed                 = 15 * 1024 * 1024
)

type SimpleAttribute struct {
	attributes map[string]string
}

func (a *SimpleAttribute) Get(name string) (string, error) {
	if v, ok := a.attributes[name]; !ok {
		return v, nil
	} else {
		return "", errors.Errorf("not exist attribute %s", name)
	}
}

type condEvent struct {
	timestamp time.Time
	cond      v1.NodeCondition
}

type HaDecisionTree struct {
	Reason       common.HaReason
	InsAvailable InsAvailableDecision
}

func (t *HaDecisionTree) Name() string {
	return "HaDecisionTree"
}

func (t *HaDecisionTree) SelectNewMasterFromStandby(m *meta.MetaManager, s *status.StatusManager) (string, error) {
	clusterSpec := m.ClusterSpec
	clusterMeta := m.ClusterMeta
	clusterStatus := s.ClusterStatus
	rwEndpoint := clusterMeta.SubCluster[clusterMeta.MasterClusterID].RwEndpoint
	rwTimestamp, rwTimestampExist := clusterStatus.SubCluster[clusterMeta.MasterClusterID].EngineStatus[rwEndpoint][common.EngineEventCategory].Value[common.EventTimestamp]
	rwSpec := clusterSpec.SubCluster[clusterMeta.MasterClusterID].InstanceSpec[rwEndpoint.String()]

	var maxCommitIndex int64 = 0
	maxCommitIndexCluster := ""
	backup := ""

	for clusterID, subCluster := range clusterMeta.SubCluster {
		spec := clusterSpec.SubCluster[clusterID].InstanceSpec[subCluster.RwEndpoint.String()]
		engineStatus := clusterStatus.SubCluster[clusterID].EngineStatus[subCluster.RwEndpoint]
		// Readonly节点的priority=-1，不允许切换为主节点
		if spec.SwitchPriority == -1 {
			log.Debugf("skip standby %s since switch priority", spec.String())
			continue
		}
		if clusterID != clusterMeta.MasterClusterID {
			if spec.ClusterType == common.PaxosCluster {
				if backup == "" {
					backup = clusterID
				}
				if paxosRole, exist := engineStatus[common.EngineMetricsEventCategory].Value[common.EventPaxosRole]; exist {
					if paxosRole == 2 {
						log.Infof("%s paxos role %d, may it failover already", spec.String(), paxosRole)
						return clusterID, nil
					}
				}
				if commitIndex, exist := engineStatus[common.EngineMetricsEventCategory].Value[common.EventCommitIndex]; exist {
					if commitIndex.(int64) > maxCommitIndex {
						maxCommitIndex = commitIndex.(int64)
						maxCommitIndexCluster = clusterID
					}
				}
			} else if subCluster.EnginePhase[subCluster.RwEndpoint.String()] == common.EnginePhaseRunning &&
				engineStatus[common.EngineEventCategory].Name == status.EngineStatusAlive {

				// 同步延迟小于30s，丢数据<30s，否则不切
				if delay, exist := engineStatus[common.EngineDelayEventCategory].Value[common.EventWriteDelayTime]; exist {
					if delay.(int) < *common.StandbyRPO {
						log.Infof("standby %s delay %d is less then %d", spec.String(), delay.(int), *common.StandbyRPO)
						return clusterID, nil
					} else {
						log.Debugf("skip standby %s delay %d is more then %d", spec.String(), delay.(int), *common.StandbyRPO)
					}
				}
				// 处理没有同步延迟数据的场景
				if standbyTimestamp, exist := engineStatus[common.EngineEventCategory].Value[common.EventTimestamp]; exist {
					if rwTimestampExist {
						if rwTimestamp.(int64)-standbyTimestamp.(int64) < int64(*common.StandbyRPO) {
							log.Infof("standby ts %d master ts %d is less then %d", standbyTimestamp.(int64), rwTimestamp.(int64), *common.StandbyRPO)
							return clusterID, nil
						} else {
							log.Debugf("Skip standby %s master ts %d standby ts %d more then %d",
								spec.String(), rwTimestamp.(int64), standbyTimestamp.(int64), *common.StandbyRPO)
						}
					}
					tagMap := m.GetEngineTag(rwSpec)
					if tagMap != nil {
						if v, exist := tagMap[common.EventTimestamp]; exist {
							ts, err := strconv.ParseInt(v, 10, 64)
							if err == nil {
								if ts-standbyTimestamp.(int64) < int64(*common.StandbyRPO) {
									log.Infof("standby ts %d master ts %d is less then %d", standbyTimestamp.(int64), ts, *common.StandbyRPO)
									return clusterID, nil
								} else {
									log.Debugf("Skip standby %s master ts %d standby ts %d more then %d",
										spec.String(), ts, standbyTimestamp.(int64), *common.StandbyRPO)
								}
							} else {
								log.Warnf("invalid tag delay %s", v)
							}
						} else {
							log.Warnf("rw has no %s", common.EventTimestamp)
						}
					}
				}
			}
		}
	}

	if maxCommitIndexCluster != "" {
		return maxCommitIndexCluster, nil
	} else if backup != "" {
		return backup, nil
	}

	return "", errors.New("All replica is down")
}

func (t *HaDecisionTree) SelectNewRwFromReplica(clusterMeta *common.ClusterStatusMeta, clusterStatus *status.ClusterStatus, clusterID string) (*common.EndPoint, error) {
	var newRw common.EndPoint
	minDelay := 1024 * 1024 * 1024
	hasOnline := false

	for endpoint, enginePhase := range clusterMeta.SubCluster[clusterID].EnginePhase {
		engineStatus := clusterStatus.SubCluster[clusterID].EngineStatus[*common.NewEndPointWithPanic(endpoint)]
		if endpoint != clusterMeta.SubCluster[clusterID].RwEndpoint.String() &&
			enginePhase == common.EnginePhaseRunning &&
			engineStatus[common.EngineEventCategory].Name == status.EngineStatusAlive {

			if newRw.IsDefault() {
				newRw = *common.NewEndPointWithPanic(endpoint)
			}

			online := false
			onlineV, exist := engineStatus[common.EngineMetricsEventCategory].Value[common.EventOnlinePromote]
			if exist {
				online = onlineV.(bool)
			}
			if !*common.EnableOnlinePromote {
				online = false
			}

			if hasOnline {
				if online {
					if delay, exist := engineStatus[common.EngineDelayEventCategory].Value[common.EventReplayDelaySize]; exist {
						if delay.(int) < minDelay {
							minDelay = delay.(int)
							newRw = *common.NewEndPointWithPanic(endpoint)
						}
						log.Debugf("engine %s delay %d", endpoint, delay)
					}
				}

			} else {
				if online {
					if delay, exist := engineStatus[common.EngineDelayEventCategory].Value[common.EventReplayDelaySize]; exist {
						minDelay = delay.(int)
						log.Debugf("engine %s delay %d", endpoint, delay)
					}
					newRw = *common.NewEndPointWithPanic(endpoint)
					hasOnline = true
				} else {
					if delay, exist := engineStatus[common.EngineDelayEventCategory].Value[common.EventReplayDelaySize]; exist {
						if delay.(int) < minDelay {
							minDelay = delay.(int)
							newRw = *common.NewEndPointWithPanic(endpoint)
						}
						log.Debugf("engine %s delay %d", endpoint, delay)
					}

				}
			}

		}
	}

	if newRw.IsDefault() {
		return nil, errors.New("All replica is down")
	} else {
		return &newRw, nil
	}
}

func CheckLastNReason(actual []string, expect string, n int) bool {
	for i, s := range actual {
		if len(actual)-i <= n && s == expect {
			return true
		}
	}
	return false
}

func CheckReason(actual []string, expect string) bool {
	return CheckLastNReason(actual, expect, 3)
}

func (t *HaDecisionTree) GetInsFailReason(s *status.SubClusterStatus, ins common.EndPoint) string {
	if v, exist := s.EngineStatus[ins]; exist {
		engineState := v[common.EngineEventCategory]
		if CheckReason(engineState.Reason, common.EngineReadOnly) {
			return common.EngineReadOnly
		}
		if CheckReason(engineState.Reason, common.EngineStart) {
			return common.EngineStart
		}
	}
	return detector.EngineAliveEvent
}

func (t *HaDecisionTree) Evaluate(m *meta.MetaManager, s *status.StatusManager, statusEvent *status.StatusEvent) ([]action.ActionExecutor, error) {
	clusterSpec := m.ClusterSpec
	clusterMeta := m.ClusterMeta
	clusterStatus := s.ClusterStatus

	var actions []action.ActionExecutor

	if statusEvent.Type == status.ProxyStatusType {
		return actions, nil
	}
	if !*common.EnableAllAction {
		return actions, nil
	}

	if common.IsHaManualMode() {
		return actions, nil
	}

	// 1. 判断master cluster可用性
	if _, exist := clusterStatus.SubCluster[clusterMeta.MasterClusterID]; exist {
		// 1.1 判断RW节点可用性
		rwSpec, err := m.GetRwSpec(clusterMeta.MasterClusterID)
		if err != nil {
			log.Fatalf("Failed to get master cluster %s rw err %s -- > goto standby check", clusterMeta.MasterClusterID, err.Error())
		}
		phase, phaseStartAt := m.GetRwEnginePhase()
		statusStartAt := clusterStatus.SubCluster[clusterMeta.MasterClusterID].EngineStatusStartAt[rwSpec.Endpoint]
		if phaseStartAt.After(statusStartAt) {
			log.Debugf("Skip rw[%v] ha since status startAt %s before phase [%s] startAt %s ", clusterMeta.MasterClusterID, statusStartAt.String(), phase, phaseStartAt.String())
			goto SkipRW
		}
		if available, _ := t.InsAvailable.EvaluateInsAvailable(clusterStatus.SubCluster[clusterMeta.MasterClusterID], rwSpec, common.RwEngine); phase == common.EnginePhasePending && !available {

			if t.GetInsFailReason(clusterStatus.SubCluster[clusterMeta.MasterClusterID], rwSpec.Endpoint) == common.EngineReadOnly {
				log.Info("generate promote action cause rw in readonly.")

				rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
				rEvent.Body.Describe = fmt.Sprintf("Instance promotion task created, promotion to rw, ins: %v", rwSpec.PodName)
				rEvent.EventCode = notify.EventCode_PormotionTaskCreated
				rEvent.Level = notify.EventLevel_INFO

				sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
				if sErr != nil {
					log.Errorf("Failed to notify.MsgNotify.SendMsgToV2 err: %v", sErr)
				}

				if ac, err := action.GeneratePromoteAction(m); err != nil {
					return nil, err
				} else {
					actions = append(actions, ac)
				}
				return actions, nil
			} else {
				// 否则简单尝试重启rw

				rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
				rEvent.Body.Describe = fmt.Sprintf("Instance restart task created,  ins: %v", rwSpec.PodName)
				rEvent.EventCode = notify.EventCode_RestartTaskCreated
				rEvent.Level = notify.EventLevel_INFO

				sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
				if sErr != nil {
					log.Errorf("Failed to notify.MsgNotify.SendMsgToV2 err: %v", sErr)
				}

				if ac, err := action.GenerateRestartAction(
					clusterSpec.SubCluster[clusterMeta.MasterClusterID].InstanceSpec[rwSpec.Endpoint.String()],
					common.RwEngine); err != nil {
					return nil, err
				} else {
					actions = append(actions, ac)
				}
				return actions, nil
			}
			//}
		}
		if available, reason := t.InsAvailable.EvaluateInsAvailable(clusterStatus.SubCluster[clusterMeta.MasterClusterID], rwSpec, common.RwEngine); (phase == common.EnginePhaseRunning || phase == common.EnginePhaseFailed) && phaseStartAt.Before(statusStartAt) && !available {

			rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
			rEvent.Body.Describe = fmt.Sprintf("Instance %v unavailable . po: %s, ep: %s , AutoReplicaFailover:%v", rwSpec.CustID, rwSpec.PodName, rwSpec.Endpoint, *common.AutoReplicaFailover)
			rEvent.EventCode = notify.EventCode_InstanceUnavailable
			rEvent.Level = notify.EventLevel_ERROR

			sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
			if sErr != nil {
				log.Errorf("_evaluateInsRunOrFailed err: %v", sErr)
			}

			// 1.2 判断是否开启replica failover
			if *common.AutoReplicaFailover {
				// 1.2.1 从replica中选择new ew
				newRw, err := t.SelectNewRwFromReplica(clusterMeta, clusterStatus, clusterMeta.MasterClusterID)
				if err != nil {
					log.Warnf("Failed to select new rw cause %s!", err.Error())
					// 如果主库是readonly错误，代表主库可能是因为promote操作未成功，因此直接执行promote操作
					if t.GetInsFailReason(clusterStatus.SubCluster[clusterMeta.MasterClusterID], rwSpec.Endpoint) == common.EngineReadOnly {
						log.Info("generate promote action cause rw in readonly.")
						if ac, err := action.GeneratePromoteAction(m); err != nil {

							rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
							rEvent.Body.Describe = fmt.Sprintf("Instance promotion task created err: %v, promotion to rw, ins: %v, reason: %s", err, rwSpec.PodName, reason.String())
							rEvent.EventCode = notify.EventCode_PormotionTaskCreated
							rEvent.Level = notify.EventLevel_ERROR

							sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
							if sErr != nil {
								log.Errorf("_evaluateInsRunOrFailed err: %v", sErr)
							}

							return nil, err
						} else {
							rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
							rEvent.Body.Describe = fmt.Sprintf("Instance promotion task created, promotion to rw, ins: %v, reason: %s", rwSpec.PodName, reason.String())
							rEvent.EventCode = notify.EventCode_PormotionTaskCreated
							rEvent.Level = notify.EventLevel_INFO

							sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
							if sErr != nil {
								log.Errorf("_evaluateInsRunOrFailed err: %v", sErr)
							}

							actions = append(actions, ac)
						}
						return actions, nil
					} else {
						// 公有云模式，尝试重启
						if resource.IsCloudOperator() {
							// 否则简单尝试重启rw
							if ac, err := action.GenerateRestartAction(
								clusterSpec.SubCluster[clusterMeta.MasterClusterID].InstanceSpec[rwSpec.Endpoint.String()],
								common.RwEngine); err != nil {
								return nil, err
							} else {
								actions = append(actions, ac)
							}
						}
						// 一体机模式，由PhaseDecision标记rw/ro都为failed，由管控重建
						return actions, nil
					}
				} else {
					// 1.2.2 生成切换任务
					rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
					rEvent.Body.Describe = fmt.Sprintf("Instance switch task created, promotion to rw, ins: %v, reason: %s", rwSpec.PodName, reason.String())
					rEvent.EventCode = notify.EventCode_SwitchTaskCreated
					rEvent.Level = notify.EventLevel_INFO

					sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
					if sErr != nil {
						log.Errorf("_evaluateInsRunOrFailed err: %v", sErr)
					}

					if ac, err := action.GenerateReplicaSwitchAction(m, s, reason, false, true, m.GetMasterClusterID(), newRw); err != nil {
						return nil, err
					} else {
						actions = append(actions, ac)
					}
					if *common.EnableAutoUpdatePXConf {
						ac := action.GenerateUpdatePXConfigAction(false)
						actions = append(actions, ac)
					}
					return actions, nil
				}
			} else if *common.AutoStandbyFailover {
				// 1.2.2 从standby中选择new master
				newMaster, err := t.SelectNewMasterFromStandby(m, s)
				if err != nil {
					if t.GetInsFailReason(clusterStatus.SubCluster[clusterMeta.MasterClusterID], rwSpec.Endpoint) != common.EngineStart {
						log.Warnf("Failed to select new master cause %s try to restart!", err.Error())
						if ac, err := action.GeneratePromoteAction(m); err != nil {
							return nil, err
						} else {
							actions = append(actions, ac)
						}
					} else {
						log.Warnf("Failed to select new master cause %s master in startup skip it!", err.Error())
					}
					return actions, nil
				} else {
					// 1.2.2 生成切换任务
					if ac, err := action.GenerateStandbySwitchAction(
						m, s, reason, false, true, m.GetMasterClusterID(), newMaster, *common.StandbyRPO, *common.StandbyRTO); err != nil {
						return nil, err
					} else {
						actions = append(actions, ac)
					}
					if *common.EnableAutoUpdatePXConf {
						ac := action.GenerateUpdatePXConfigAction(false)
						actions = append(actions, ac)
					}
					return actions, nil
				}
			} else {
				log.Infof("Skip master cluster %s ha decision cause disable auto failover!", clusterMeta.MasterClusterID)

				rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
				rEvent.Body.Describe = fmt.Sprintf("Instance promotion task not created cause disable auto failover,  ins: %v, reason: %s", rwSpec.PodName, reason.String())
				rEvent.EventCode = notify.EventCode_PormotionTaskCreated
				rEvent.Level = notify.EventLevel_ERROR

				sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
				if sErr != nil {
					log.Errorf("_evaluateInsRunOrFailed err: %v", sErr)
				}

			}
		}
	}

SkipRW:

	// 2. 判断standby cluster的可用性
	for clusterID, subClusterMeta := range clusterMeta.SubCluster {
		if clusterID == clusterMeta.MasterClusterID {
			continue
		}
		if subClusterMeta.EnginePhase[subClusterMeta.RwEndpoint.String()] != common.EnginePhaseRunning {
			continue
		}
		standbySpec, err := m.GetRwSpec(clusterID)
		if err != nil {
			log.Warnf("Failed to get cluster %s standby err %s", clusterID, err.Error())
			continue
		}
		// 1.1 判断standby-rw节点可用性
		if available, reason := t.InsAvailable.EvaluateInsAvailable(clusterStatus.SubCluster[clusterID], standbySpec, common.StandbyEngine); !available {
			// 1.2 判断是否开启standby failover
			if *common.AutoStandbyReplicaFailover {
				// 1.2.1 从replica中选择new rw
				newRw, err := t.SelectNewRwFromReplica(clusterMeta, clusterStatus, clusterID)
				if err != nil {
					log.Warnf("Failed to select new standby-rw cause %s!", err.Error())
				} else {
					// 1.2.2 生成切换任务
					if ac, err := action.GenerateReplicaSwitchAction(m, s, reason, false, true, clusterID, newRw); err != nil {
						return nil, err
					} else {
						actions = append(actions, ac)
					}
					if *common.EnableAutoUpdatePXConf {
						ac := action.GenerateUpdatePXConfigAction(false)
						actions = append(actions, ac)
					}
				}
			} else {
				log.Infof("Skip standby cluster %s ha decision cause disable auto failover!", clusterID)
			}
		}
	}

	return actions, nil
}
