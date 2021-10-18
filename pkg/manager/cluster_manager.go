package manager

import (
	"fmt"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/decision"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	v12 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ClusterManager struct {
	// ClusterManager停止标志
	StopFlag chan bool
	// Watch Flag ConfigMap 协程停止标志
	FlagWatchStopped chan bool
	// Watch PpasCluster 协程停止标志
	WatchClusterFlag chan bool
	// Watch MaxScale 协程停止标志
	WatchProxyFlag chan bool

	// Status变化决策模块
	StatusChangeDecisions []decision.StatusChangeDecision
	// Spec变化决策模块
	SpecChangeDecisions []decision.SpecChangeDecision

	// 状态变化channel
	StatusQueue chan status.StatusEvent
	// MaxScale规格变化channel
	ProxySpecQueue chan common.ProxySpec

	// 集群状态机管理
	StatusManager *status.StatusManager

	// 元数据存储
	MetaManager *meta.MetaManager

	// 集群管理锁，保证管理逻辑串行化
	ManagerLock sync.Mutex
	// 节点角色
	Role string
}

var once sync.Once
var cm *ClusterManager

func GetClusterManager() *ClusterManager {
	once.Do(func() {
		cm = &ClusterManager{
			StopFlag:         make(chan bool),
			FlagWatchStopped: make(chan bool),
			WatchClusterFlag: make(chan bool),
			WatchProxyFlag:   make(chan bool),
			StatusQueue:      make(chan status.StatusEvent, 1024),
			ProxySpecQueue:   make(chan common.ProxySpec),
			Role:             common.FollowerRole,
		}
		cm.MetaManager = meta.GetMetaManager()
		cm.StatusManager = status.NewStatusManager(cm.StatusQueue)
	})

	return cm
}

func (m *ClusterManager) Reset() error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	m.Role = common.FollowerRole
	m.StopFlag = make(chan bool)
	m.FlagWatchStopped = make(chan bool)
	m.WatchClusterFlag = make(chan bool)
	m.WatchProxyFlag = make(chan bool)
	m.StatusQueue = make(chan status.StatusEvent, 1024)
	m.ProxySpecQueue = make(chan common.ProxySpec)

	m.StatusManager.Reset(m.StatusQueue)
	m.MetaManager.Reset()

	m.StatusChangeDecisions = m.StatusChangeDecisions[:0]
	m.SpecChangeDecisions = m.SpecChangeDecisions[:0]

	return nil
}

func (m *ClusterManager) Initialize() error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	m.Role = common.LeaderRole

	if err := m.MetaManager.Reload(); err != nil {
		log.Fatalf("Failed reload meta manager err %s", err.Error())
	}

	var actions []action.ActionExecutor
	for clusterID, subCluster := range m.MetaManager.ClusterMeta.SubCluster {
		for ep := range subCluster.EnginePhase {
			spec, err := m.MetaManager.GetInsSpec(common.NewEndPointWithPanic(ep))
			if err != nil {
				log.Fatalf("Failed to initialize meta manager err %s", err.Error())
			}
			if ep == subCluster.RwEndpoint.String() {
				rwSpec, err := m.MetaManager.GetRwSpec(m.MetaManager.GetMasterClusterID())
				if err != nil {
					log.Fatalf("Failed to initialize meta manager err %s", err.Error())
				}
				if clusterID == m.MetaManager.ClusterMeta.MasterClusterID {
					actions = append(actions, &action.AddInsActionExecutor{
						InsSpec:    *spec,
						RwSpec:     *rwSpec,
						EngineType: common.RwEngine,
					})
				} else {
					actions = append(actions, &action.AddInsActionExecutor{
						InsSpec:    *spec,
						RwSpec:     *rwSpec,
						EngineType: common.StandbyEngine,
					})
				}
			} else {
				rwSpec, err := m.MetaManager.GetInsSpec(common.NewEndPointWithPanic(subCluster.RwEndpoint.String()))
				if err != nil {
					log.Fatalf("Failed to initialize meta manager err %s", err.Error())
				}
				actions = append(actions, &action.AddInsActionExecutor{
					InsSpec:    *spec,
					RwSpec:     *rwSpec,
					EngineType: common.RoEngine,
				})
			}
		}
	}

	for _, ac := range actions {
		log.Infof("Success to generate action %s by decision Initialize", ac.String())
		err := m.ExecuteAction(ac)
		if err != nil {
			m.AlarmActionFailed(ac, err)
			log.Warnf("Failed to execute action %s err %s", ac.String(), err.Error())
		}
		log.Debugf("CurMeta: %s", m.MetaManager.String())
	}

	var runningTask common.SwitchTask
	var phase string
	var vips []common.VipSpec

	if resource.IsPolarPureMode() || resource.IsPolarPaasMode() {
		runningTask = m.MetaManager.GetRunningTask()
		phase = m.MetaManager.GetPhase()
		vips = m.MetaManager.GetVips()
		for _, vip := range vips {
			err := m.AddVip(vip.Vip, vip.Interface, vip.Mask)
			if err != nil {
				log.Fatalf("Failed to add vip %v err %s", vip, err.Error())
			}
		}
	} else {
		clusterManagerConfig, err := resource.GetResourceManager().GetK8sClient().GetConfigMap()
		if err != nil {
			log.Fatalf("Failed to get cluster map err %s", err.Error())
		}
		runningTask = clusterManagerConfig.RunningTask
		phase = clusterManagerConfig.Phase
	}

	if runningTask.Phase == common.SwitchingPhase {
		// 恢复异常中止的切换任务，需要设置为强制切换，保证最终的状态一致
		runningTask.Action.Force = true
		ac := action.NewSwitchActionExecutor(runningTask.Action, runningTask.Reason)
		log.Infof("Success to generate action %s by decision Recovery", ac.String())
		err := m.ExecuteAction(ac)
		if err != nil {
			m.AlarmActionFailed(ac, err)
			log.Warnf("Failed to execute action %s err %s", ac.String(), err.Error())
		}
	}

	topology := m.MetaManager.GetTopologyFromMetaManager()
	smartclientDesicion := &decision.SmartClientDecision{CurrentTopology: topology}

	m.SpecChangeDecisions = append(m.SpecChangeDecisions, &decision.InsChangeDecision{})

	if resource.GetClusterManagerConfig().Mode == common.POLAR_BOX_ONE {
		m.SpecChangeDecisions = append(m.SpecChangeDecisions, &decision.LockReadOnlyDecision{})
		m.SpecChangeDecisions = append(m.SpecChangeDecisions, &decision.SmartClientSpecDecision{SmartClientDecision: smartclientDesicion})

		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.BoxRebuildRwDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.BoxEventDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.StorageFullDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.ConfConsistentDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.SmartClientStatusDecision{SmartClientDecision: smartclientDesicion})
	} else if resource.GetClusterManagerConfig().Mode == common.POLAR_CLOUD {
		// the order is very important since action maybe impact next decision
		// so the HA decision should be first order
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.HaDecisionTree{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.ProxyHaDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.PhaseDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.RoDelayDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.SQLMonitorDecision{})
	} else if resource.GetClusterManagerConfig().Mode == common.POLAR_BOX_MUL {
		m.SpecChangeDecisions = append(m.SpecChangeDecisions, &decision.LockReadOnlyDecision{})
		m.SpecChangeDecisions = append(m.SpecChangeDecisions, &decision.SmartClientSpecDecision{SmartClientDecision: smartclientDesicion})

		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.HaDecisionTree{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.ProxyHaDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.PhaseDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.RoDelayDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.BoxEventDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.StorageFullDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.ConfConsistentDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.SmartClientStatusDecision{SmartClientDecision: smartclientDesicion})

	} else if resource.GetClusterManagerConfig().Mode == common.POLAR_PAAS_MS {
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.HaDecisionTree{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.PhaseDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.StandbyDelayDecision{})

		*common.AutoStandbyFailover = true
	} else if resource.GetClusterManagerConfig().Mode == common.POLAR_PURE {
		m.SpecChangeDecisions = append(m.SpecChangeDecisions, &decision.StopClusterDecision{})
		//m.SpecChangeDecisions = append(m.SpecChangeDecisions, &decision.TopologyChangeDecision{})

		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.HaDecisionTree{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.PhaseDecision{})
		m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.RoDelayDecision{})

		if resource.GetResourceManager().GetOperatorClient().GetType() == "mpd" {
			*common.AutoStandbyFailover = false
			m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.ConfConsistentDecision{})
		} else if resource.GetResourceManager().GetOperatorClient().GetType() == "cloud" {
			m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.StandbyDelayDecision{})
			*common.AutoStandbyFailover = false
		} else {
			m.StatusChangeDecisions = append(m.StatusChangeDecisions, &decision.StandbyDelayDecision{})
			*common.AutoStandbyFailover = true
		}
	} else {
		log.Fatalf("UnSupport work mode %s", resource.GetClusterManagerConfig().Mode)
	}

	if phase == common.DisableAllPhase {
		err := m.EnableWithoutLock(false)
		if err != nil {
			log.Warnf("Failed to initial phase to %s err=%s", phase, err.Error())
		}
	} else {
		err := m.EnableWithoutLock(true)
		if err != nil {
			log.Warnf("Failed to initial phase to %s err=%s", phase, err.Error())
		}
	}

	return nil
}

func (m *ClusterManager) Start() {
	go m.ReconcileLoop()
}

func (m *ClusterManager) Stop() {
	m.WatchClusterFlag <- true
	m.FlagWatchStopped <- true
	m.StopFlag <- true

	resource.GetResourceManager().SmartClientService.Stop()
}

func (m *ClusterManager) WatchFlagChange() {
	log.Infof("Start watch flag change!")
	defer log.Infof("Stop watch flag change!")

	defer m.CrashRecoveryAlarmAction()

	var timeout int64 = 600

	c, _, err := resource.GetKubeClient(time.Duration(timeout+10) * time.Second)
	if err != nil {
		log.Errorf("WatchFlagChange raise panic err: %v", err)
		panic(err)
	}

	c.CoreV1().ConfigMaps(resource.GetClusterManagerConfig().Cluster.Namespace).Create(&v12.ConfigMap{
		ObjectMeta: v1.ObjectMeta{
			Name: resource.GetClusterManagerConfig().Cluster.Name + common.FlagConfigMapPostfix,
		},
	})

	watchFinFlag := make(chan bool)
	for {
		time.Sleep(time.Second * 5)

		cmName := resource.GetClusterManagerConfig().Cluster.Name + common.FlagConfigMapPostfix
		watch, err := c.CoreV1().ConfigMaps(resource.GetClusterManagerConfig().Cluster.Namespace).Watch(v1.ListOptions{
			FieldSelector:  fields.OneTermEqualSelector("metadata.name", cmName).String(),
			TimeoutSeconds: &timeout,
		})
		if err != nil {
			log.Warnf("Failed to watch %s flag configmap err %s", cmName, err.Error())
			continue
		}

		go func() {
			for event := range watch.ResultChan() {
				flags, ok := event.Object.(*v12.ConfigMap)
				if !ok {
					log.Warnf("unexpected type[%v] %v ", event.Type, event.Object)
					break
				} else {
					if resource.IsPolarBoxOneMode() {
						if v, exist := flags.Data[common.EnableAllFlagType]; exist {
							if v == "0" {
								err = m.Enable(false)
							} else {
								err = m.Enable(true)
							}
							if err != nil {
								log.Warnf("Failed to switch phase err=%s", err.Error())
							}
						}
					}
					if v, exist := flags.Data[common.EnableRedLineType]; exist {
						if v == "0" {
							*common.EnableRedLine = false
						} else {
							*common.EnableRedLine = true
						}
						log.Infof("EnableRedLine to %v", *common.EnableRedLine)
					}
					if v, exist := flags.Data[common.EnableRebuildRoStandbyType]; exist {
						if v == "0" {
							*common.EnableRebuildRoStandby = false
						} else {
							*common.EnableRebuildRoStandby = true
						}
					}
					if v, exist := flags.Data[common.EnableRebuildRoStandbyType]; exist {
						*common.EnableRebuildRoStandby = (v != "0")
						log.Infof("EnableRebuildRoStandby to %v", *common.EnableRebuildRoStandby)
					}
					if v, exist := flags.Data[common.EngineDetectTimeoutFlagType]; exist {
						timeout, err := strconv.Atoi(v)
						if err == nil && timeout <= 20000 && timeout >= 500 && *common.EngineDetectTimeoutMs != int64(timeout) {
							*common.EngineDetectTimeoutMs = int64(timeout)
							log.Infof("Success to dynamic config %s to %s", common.EngineDetectTimeoutFlagType, v)
						} else {
							log.Warnf("Failed to dynamic config %s to %s err %v", common.EngineDetectTimeoutFlagType, v, err)
						}
					}
					if v, exist := flags.Data[common.SQLMonitorFlagType]; exist {
						err := resource.GetConfigManager().UpdateSQLMonitorConfig(v)
						if err != nil {
							log.Warnf("Failed to dynamic config %s to %s err %v", common.SQLMonitorFlagType, v, err)
						} else {
							log.Infof("Success to dynamic config %s to %s", common.SQLMonitorFlagType, v)
						}
					}
					if v, exist := flags.Data[common.EngineReplicaDelayMsFlagType]; exist {
						timeout, err := strconv.Atoi(v)
						if err == nil && timeout <= 3600000 && timeout >= 1000 && *common.EngineReplicaDelayMs != int64(timeout) {
							*common.EngineReplicaDelayMs = int64(timeout)
							log.Infof("Success to dynamic config %s to %s", common.EngineReplicaDelayMsFlagType, v)
						} else {
							log.Warnf("Failed to dynamic config %s to %s err %v", common.EngineReplicaDelayMsFlagType, v, err)
						}
					}
					if v, exist := flags.Data[common.EnableAutoUpdatePXConfType]; exist {
						if v == "0" {
							m.EnableAutoUpdatePXConf(false)
						} else {
							m.EnableAutoUpdatePXConf(true)
						}
					}
					if v, exist := flags.Data[common.EnableOnlinePromoteFlagType]; exist {
						if v == "0" {
							*common.EnableOnlinePromote = false
							log.Infof("Get EnableOnlinePromote = false")
						} else {
							*common.EnableOnlinePromote = true
							log.Infof("Get EnableOnlinePromote = true")
						}
					}
				}
			}
			watchFinFlag <- true
		}()
		select {
		case <-watchFinFlag:
			watch.Stop()
		case <-m.FlagWatchStopped:
			log.Info("Starting stop watch flag configmap!")
			watch.Stop()
			return
		}
	}
}

func (m *ClusterManager) ExecuteAction(actionExecutor action.ActionExecutor) error {
	if actionExecutor != nil {
		err := actionExecutor.Execute(m.MetaManager, m.StatusManager)
		if err != nil {
			return err
		}
		log.Infof("Success to exec action %s", actionExecutor.String())
	}
	return nil
}

func (m *ClusterManager) TriggerStatusChangeAction(statusEvent *status.StatusEvent) {
	for _, onedecision := range m.StatusChangeDecisions {
		m._execStatusOneDecision(onedecision, statusEvent)
	}
}

func (m *ClusterManager) _execStatusOneDecision(oneDecision decision.StatusChangeDecision, statusEvent *status.StatusEvent) {
	defer func() {
		r := recover()
		if r != nil {
			log.Errorf("[_execStatusOneDecision] panic StatusChangeDecision %s (%v) error : %v", oneDecision.Name(), statusEvent.SimpleString(), r)
			debug.PrintStack()
		}
	}()
	begin := time.Now()
	defer func() {
		spend := time.Since(begin).Seconds()
		if spend >= 2 {
			log.Infof("[_execStatusOneDecision]: %v (%v),start :%v ,spend: %v s", oneDecision.Name(), statusEvent.SimpleString(), begin, spend)
		}
	}()
	if executors, err := oneDecision.Evaluate(m.MetaManager, m.StatusManager, statusEvent); err != nil {
		log.Errorf("[_execStatusOneDecision]Evaluate StatusChangeDecision %s (%v) error : %v", oneDecision.Name(), statusEvent.SimpleString(), err.Error())
	} else {
		lExecutors := len(executors)
		if lExecutors > 0 {
			var eNames []string
			for _, executor := range executors {
				eNames = append(eNames, executor.String())
			}
			log.Infof("_execStatusOneDecision[%v]  executorNum:= %v [%s] (%v)", oneDecision.Name(), lExecutors, strings.Join(eNames, ","), statusEvent.SimpleString())

			log.Debugf("CurStatus: %s", m.StatusManager.ClusterStatus.String(nil))
		}

		for _, executor := range executors {
			log.Infof("[_execStatusOneDecision]ExecuteAction action[%v]--> %s event: (%v)", oneDecision.Name(), executor.String(), statusEvent.SimpleString())

			b := time.Now()
			err = m.ExecuteAction(executor)
			spend := time.Since(b).Seconds()
			if err != nil {
				log.Infof("[_execStatusOneDecision]ExecuteAction [%s] --> %s event: (%v) err: %s", oneDecision.Name(), executor.String(), statusEvent.SimpleString(), err.Error())
				m.AlarmActionFailed(executor, err)
			} else {
				m.RemoveActionFailedAlarm(executor)
			}
			log.Debugf("[_execStatusOneDecision][%v/%v] (spend:%v s) CurMeta: %s", oneDecision.Name(), executor.String(), spend, m.MetaManager.String())
		}
	}
}

func (m *ClusterManager) TriggerSpecChangeAction() {
	for _, decision := range m.SpecChangeDecisions {
		m._execSpecOneDecision(decision)
	}
}

func (m *ClusterManager) _execSpecOneDecision(oneDecision decision.SpecChangeDecision) {
	begin := time.Now()
	defer func() {
		spend := time.Now().Sub(begin).Seconds()
		if spend >= 2 {
			log.Infof("[_execSpecOneDecision]: %v ,start :%v ,spend: %v s", oneDecision.Name(), begin, spend)
		}
	}()
	if executors, err := oneDecision.Evaluate(m.MetaManager, m.StatusManager, &common.SpecEvent{}); err != nil {
		log.Error("Evaluate SpecChangeDecision error ", err)
	} else {
		lExecutors := len(executors)
		if lExecutors > 0 {
			log.Infof("_execSpecOneDecision[%v] executorNum:= %v", oneDecision.Name(), lExecutors)
		}
		for _, executor := range executors {
			log.Infof("Success to generate action %s by decision %s", executor.String(), oneDecision.Name())
			err = m.ExecuteAction(executor)
			if err != nil {
				log.Warnf("Failed to execute action %s err %s", executor.String(), err.Error())
				m.AlarmActionFailed(executor, err)
			} else {
				m.RemoveActionFailedAlarm(executor)
			}
			log.Debugf("[_execSpecOneDecision][%v/%v]CurMeta: %s", executor.String(), oneDecision.Name(), m.MetaManager.String())
		}
	}
}

func (m *ClusterManager) AlarmActionFailed(ac action.ActionExecutor, err error) {
	if err != nil {
		eventType := common.AlarmSmsEventType
		alarmAction := &action.AlarmActionExecutor{
			Alarm: common.AlarmInfo{
				AlarmMsg: common.AlarmMsg{
					EventType:   eventType,
					Message:     "Failed to exec " + ac.String() + " err " + err.Error() + " at " + time.Now().String(),
					ClusterName: resource.GetClusterManagerConfig().Cluster.Name,
					Tags:        map[string]string{},
				},
				Type:    common.AlarmActionExecFailed + ":" + ac.String(),
				Spec:    common.InsSpec{},
				StartAt: time.Now(),
			},
			Op: common.AddAlarm,
		}
		err := alarmAction.Execute(m.MetaManager, m.StatusManager)
		if err != nil {
			log.Warnf("Failed to exec alarm action %s err %s", alarmAction.String(), err.Error())
		}
	}
}

func (m *ClusterManager) RemoveActionFailedAlarm(ac action.ActionExecutor) {
	alarmAction := &action.AlarmActionExecutor{
		Alarm: common.AlarmInfo{
			Type:    common.AlarmActionExecFailed + ":" + ac.String(),
			Spec:    common.InsSpec{},
			StartAt: time.Now(),
		},
		Op: common.RemoveAlarm,
	}
	err := alarmAction.Execute(m.MetaManager, m.StatusManager)
	if err != nil {
		log.Warnf("Failed to remove alarm action %s err %s", alarmAction.String(), err.Error())
	}
}

func (m *ClusterManager) CrashRecoveryAlarmAction() {
	if err := recover(); err != nil {
		eventType := common.AlarmSmsEventType
		alarmAction := &action.AlarmActionExecutor{
			Alarm: common.AlarmInfo{
				AlarmMsg: common.AlarmMsg{
					EventType:   eventType,
					Message:     "goroutine crash by " + fmt.Errorf("%v", err).Error() + " at " + time.Now().String(),
					ClusterName: resource.GetClusterManagerConfig().Cluster.Name,
					Tags:        map[string]string{},
				},
				Type:    common.AlarmCrash,
				Spec:    common.InsSpec{},
				StartAt: time.Now(),
			},
			Op: common.AddAlarm,
		}
		actionErr := alarmAction.Execute(m.MetaManager, m.StatusManager)
		if actionErr != nil {
			log.Warnf("Failed to exec alarm action %s err %s", alarmAction.String(), actionErr.Error())
		}
		var buf [4096]byte
		bufSize := runtime.Stack(buf[:], false)
		log.Fatalf("goroutine crash by %v stack %s", err, string(buf[:bufSize]))
	}
}

func (m *ClusterManager) ReconcileProxySpecChange(newSpec *common.ProxySpec) {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	if newSpec == nil {
		newSpec = &common.ProxySpec{
			InstanceSpec: map[common.EndPoint]*common.InsSpec{},
		}

		for ep, spec := range m.MetaManager.ProxyMeta.InstanceSpec {
			newSpec.InstanceSpec[*common.NewEndPointWithPanic(ep)] = spec
		}
	}

	for endpoint, insSpec := range newSpec.InstanceSpec {
		if _, exist := m.MetaManager.ProxySpec.InstanceSpec[endpoint]; !exist {
			rwSpec, err := m.MetaManager.GetRwSpec(m.MetaManager.GetMasterClusterID())
			if err != nil {
				log.Warnf("Failed to get rw spec err=%s", err.Error())
				return
			}

			log.Infof("ClusterManager add proxy %s", endpoint.String())
			m.StatusManager.CreateOrUpdateProxyStatus(insSpec, rwSpec)
			m.StatusManager.ClusterStatus.Proxy[endpoint] = map[common.EventCategory]status.State{
				common.EngineEventCategory: status.State{Name: status.ProxyStatusInit},
			}
			m.MetaManager.ProxySpec.InstanceSpec[endpoint] = insSpec
		}
	}

	for endpoint, _ := range m.MetaManager.ProxySpec.InstanceSpec {
		if _, exist := newSpec.InstanceSpec[endpoint]; !exist {
			log.Infof("ClusterManager remove proxy %s", endpoint.String())
			m.StatusManager.DeleteProxyStatus(endpoint)
			delete(m.StatusManager.ClusterStatus.Proxy, endpoint)
			delete(m.MetaManager.ProxySpec.InstanceSpec, endpoint)
		}
	}
}

func (m *ClusterManager) ReconcileClusterStatusChange(event *status.StatusEvent) {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	m.StatusManager.UpdateClusterStatus(event)

	m.TriggerStatusChangeAction(event)
}

func (m *ClusterManager) ReconcileClusterSpecChange() {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	m.TriggerSpecChangeAction()
}

func (m *ClusterManager) ReconcileLoop() error {
	log.Info("ClusterManager reconcile loop start!")
	defer log.Info("ClusterManager reconcile loop quit!")

	defer m.CrashRecoveryAlarmAction()
	for {
		select {
		case <-m.StopFlag:
			log.Infof("Get a stop info!!")
			return nil

		case newProxySpec := <-m.ProxySpecQueue:
			m.ReconcileProxySpecChange(&newProxySpec)

		case statusEvent := <-m.StatusQueue:
			m.ReconcileClusterSpecChange()
			if resource.IsPolarPureMode() {
				m.ReconcileProxySpecChange(nil)
			}

			m.ReconcileClusterStatusChange(&statusEvent)
			if resource.IsPolarPureMode() {
				m.ReconcileProxySpecChange(nil)
			}

		case <-time.After(time.Second):
			m.ReconcileClusterSpecChange()
			if resource.IsPolarPureMode() {
				m.ReconcileProxySpecChange(nil)
			}
		}
	}

	return nil
}
