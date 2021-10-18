package status

import (
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/collector"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"sync"
	"time"
)

const (
	MasterPodStatus string = "MasterPodStatus"
)

const (
	PodStatusCrash   string = "PodStatusCrash"
	PodStatusRunning string = "PodStatusRunning"
	PodStatusUnknown string = "PodStatusUnknown"
	PodStatusTimeout string = "PodStatusTimeout"

	PodStatusCpuFull   = "PodStatusCpuFull"
	PodStatusCpuNormal = "PodStatusCpuNormal"
	PodStatusCpuEmpty  = "PodStatusCpuEmpty"

	PodStatusStorageFull      = "PodStatusStorageFull"
	PodStatusStorageNormal    = "PodStatusStorageNormal"
	PodStatusNetworkBond1Down = "PodStatusNetworkBond1Down"
	PodStatusNetworkBond1Up   = "PodStatusNetworkBond1Up"

	PodMetricsCollecting = "PodMetricsCollecting"
	PodMetricsCollected  = "PodMetricsCollected"
)

type PodStatus struct {
	spec           common.InsSpec
	fsm            map[common.EventCategory]*StateMachine
	state          map[common.EventCategory]*State
	phase          string
	statusQueue    chan StatusEvent
	EventProducers []common.EventProducer
	StopFlag       bool
	Mutex          sync.Mutex
	StartAt        time.Time
}

func NewPodStatus(spec common.InsSpec, eventProducers []common.EventProducer, queue chan StatusEvent) *PodStatus {
	if len(eventProducers) == 0 {
		return nil
	}

	podTrans := []Transition{
		{From: PodStatusUnknown, Event: collector.PodEngineRunningEvent, To: PodStatusRunning, Action: &TimesTransition{0}},
		{From: PodStatusRunning, Event: collector.PodEngineRestartEvent, To: PodStatusCrash, Action: &TimesTransition{0}},
		{From: PodStatusRunning, Event: collector.PodUnknownEvent, To: PodStatusUnknown, Action: &TimesTransition{3}},
		{From: PodStatusCrash, Event: collector.PodEngineRunningEvent, To: PodStatusRunning, Action: &TimesTransition{10}},
		{From: PodStatusCrash, Event: collector.PodUnknownEvent, To: PodStatusUnknown, Action: &TimesTransition{10}},
	}

	cpuTrans := []Transition{
		{From: PodStatusUnknown, Event: collector.ContainerCpuNormalEvent, To: PodStatusCpuNormal, Action: &TimesTransition{0}},
		{From: PodStatusUnknown, Event: collector.ContainerCpuFullEvent, To: PodStatusCpuFull, Action: &TimesTransition{0}},
		{From: PodStatusUnknown, Event: collector.ContainerCpuEmptyEvent, To: PodStatusCpuEmpty, Action: &TimesTransition{0}},
		{From: PodStatusUnknown, Event: collector.ContainerTimeoutEvent, To: PodStatusTimeout, Action: &TimesTransition{3}},

		{From: PodStatusCpuNormal, Event: collector.ContainerCpuEmptyEvent, To: PodStatusCpuEmpty, Action: &TimesTransition{0}},
		{From: PodStatusCpuNormal, Event: collector.ContainerCpuFullEvent, To: PodStatusCpuFull, Action: &TimesTransition{0}},
		{From: PodStatusCpuNormal, Event: collector.ContainerUnknownEvent, To: PodStatusUnknown, Action: &TimesTransition{15}},
		{From: PodStatusCpuNormal, Event: collector.ContainerTimeoutEvent, To: PodStatusTimeout, Action: &TimesTransition{5}},

		{From: PodStatusCpuFull, Event: collector.ContainerUnknownEvent, To: PodStatusUnknown, Action: &TimesTransition{15}},
		{From: PodStatusCpuFull, Event: collector.ContainerCpuEmptyEvent, To: PodStatusCpuEmpty, Action: &TimesTransition{1}},
		{From: PodStatusCpuFull, Event: collector.ContainerCpuNormalEvent, To: PodStatusCpuNormal, Action: &TimesTransition{1}},
		{From: PodStatusCpuFull, Event: collector.ContainerTimeoutEvent, To: PodStatusTimeout, Action: &TimesTransition{5}},

		{From: PodStatusCpuEmpty, Event: collector.ContainerUnknownEvent, To: PodStatusUnknown, Action: &TimesTransition{15}},
		{From: PodStatusCpuEmpty, Event: collector.ContainerCpuNormalEvent, To: PodStatusCpuNormal, Action: &TimesTransition{0}},
		{From: PodStatusCpuEmpty, Event: collector.ContainerCpuFullEvent, To: PodStatusCpuNormal, Action: &TimesTransition{0}},
		{From: PodStatusCpuEmpty, Event: collector.ContainerTimeoutEvent, To: PodStatusTimeout, Action: &TimesTransition{3}},

		{From: PodStatusTimeout, Event: collector.ContainerUnknownEvent, To: PodStatusUnknown, Action: &TimesTransition{10}},
		{From: PodStatusTimeout, Event: collector.ContainerCpuNormalEvent, To: PodStatusCpuNormal, Action: &TimesTransition{0}},
		{From: PodStatusTimeout, Event: collector.ContainerCpuFullEvent, To: PodStatusCpuNormal, Action: &TimesTransition{0}},
		{From: PodStatusTimeout, Event: collector.ContainerCpuEmptyEvent, To: PodStatusCpuEmpty, Action: &TimesTransition{0}},
	}

	storageUsageTrans := []Transition{
		{From: PodStatusUnknown, Event: collector.StorageUsageFullEvent, To: PodStatusStorageFull, Action: &TimesTransition{0}},
		{From: PodStatusUnknown, Event: collector.StorageUsageNormalEvent, To: PodStatusStorageNormal, Action: &TimesTransition{0}},
		{From: PodStatusStorageFull, Event: collector.StorageUsageNormalEvent, To: PodStatusStorageNormal, Action: &TimesTransition{0}},
		{From: PodStatusStorageFull, Event: collector.ContainerUnknownEvent, To: PodStatusUnknown, Action: &TimesTransition{5}},
		{From: PodStatusStorageNormal, Event: collector.StorageUsageFullEvent, To: PodStatusStorageFull, Action: &TimesTransition{0}},
		{From: PodStatusStorageNormal, Event: collector.ContainerUnknownEvent, To: PodStatusUnknown, Action: &TimesTransition{5}},
	}

	networkTrans := []Transition{
		{From: PodStatusUnknown, Event: collector.PodNetworkBond1DownEvent, To: PodStatusNetworkBond1Down, Action: &TimesTransition{0}},
		{From: PodStatusUnknown, Event: collector.PodNetworkBond1UpEvent, To: PodStatusNetworkBond1Up, Action: &TimesTransition{0}},
		{From: PodStatusNetworkBond1Down, Event: collector.PodNetworkBond1UpEvent, To: PodStatusNetworkBond1Up, Action: &TimesTransition{0}},
		{From: PodStatusNetworkBond1Down, Event: collector.PodUnknownEvent, To: PodStatusUnknown, Action: &TimesTransition{5}},
		{From: PodStatusNetworkBond1Up, Event: collector.PodNetworkBond1DownEvent, To: PodStatusNetworkBond1Down, Action: &TimesTransition{0}},
		{From: PodStatusNetworkBond1Up, Event: collector.PodUnknownEvent, To: PodStatusUnknown, Action: &TimesTransition{5}},
	}

	enginePodFsm := []Transition{
		{From: EngineStatusInit, Event: collector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
		{From: EngineStatusInit, Event: collector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusAlive, Event: collector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusLosing, Event: collector.EngineFailEvent, To: EngineStatusLosing, Action: &AlwaysReportTransition{}},
		{From: EngineStatusLosing, Event: collector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
	}

	podMetricsFsm := []Transition{
		{From: PodMetricsCollecting, Event: collector.PodMetricsCollectEvent, To: PodMetricsCollected, Action: &TimesTransition{0}},
		{From: PodMetricsCollected, Event: collector.PodMetricsCollectEvent, To: PodMetricsCollecting, Action: &TimesTransition{0}},
	}

	podStatus := &PodStatus{
		spec:           spec,
		statusQueue:    queue,
		EventProducers: eventProducers,
		fsm: map[common.EventCategory]*StateMachine{
			common.PodEventCategory:          NewStateMachine(podTrans...),
			common.PodCpuEventCategory:       NewStateMachine(cpuTrans...),
			common.StorageUsageEventCategory: NewStateMachine(storageUsageTrans...),
			common.PodNetworkEventCategory:   NewStateMachine(networkTrans...),
			common.PodEngineEventCategory:    NewStateMachine(enginePodFsm...),
			common.PodMetricsEventCategory:   NewStateMachine(podMetricsFsm...),
		},
		state: map[common.EventCategory]*State{
			common.PodEventCategory:          &State{Name: PodStatusUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.PodCpuEventCategory:       &State{Name: PodStatusUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.StorageUsageEventCategory: &State{Name: PodStatusUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.PodNetworkEventCategory:   &State{Name: PodStatusUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.PodEngineEventCategory:    &State{Name: EngineStatusInit, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.PodMetricsEventCategory:   &State{Name: PodMetricsCollecting, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		},
	}

	for _, eventProducer := range eventProducers {
		eventProducer.RegisterEventCallback(podStatus)
		eventProducer.Start()
	}

	return podStatus
}

func (s *PodStatus) Stop() error {
	for _, producer := range s.EventProducers {
		producer.Stop()
	}
	return nil
}

func (s *PodStatus) ID() string {
	return s.spec.ID
}

func (s *PodStatus) Name() string {
	return "PodStatus"
}

func (s *PodStatus) EndPoint() *common.EndPoint {
	return &s.spec.Endpoint
}

func (s *PodStatus) Disable() {
	s.StopFlag = true
}

func (s *PodStatus) Enable() {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	s.state = map[common.EventCategory]*State{
		common.PodEventCategory:          &State{Name: PodStatusUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		common.PodCpuEventCategory:       &State{Name: PodStatusUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		common.StorageUsageEventCategory: &State{Name: PodStatusUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		common.PodNetworkEventCategory:   &State{Name: PodStatusUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		common.PodEngineEventCategory:    &State{Name: EngineStatusInit, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
	}

	s.StartAt = time.Now()
	for _, producer := range s.EventProducers {
		producer.ReStart()
	}

	s.StopFlag = false
}

func (s *PodStatus) GetCurState() map[common.EventCategory]State {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	curStates := map[common.EventCategory]State{}
	for k, v := range s.state {
		var reason []string
		for _, r := range v.Reason {
			reason = append(reason, r)
		}
		value := make(map[string]interface{})
		for stK, stV := range v.Value {
			value[stK] = stV
		}
		curStates[k] = State{
			Name:           v.Name,
			Reason:         reason,
			ReceivedEvents: v.ReceivedEvents,
			StartTimestamp: v.StartTimestamp,
			Value:          value,
		}
	}
	return curStates
}

func (s *PodStatus) TriggerStatusEvent(triggerEventCategory common.EventCategory) {
	if s.StopFlag {
		log.Debugf("Skip %s status event after PodStatus stop", s.spec.String())
	} else {
		s.statusQueue <- StatusEvent{
			ID:                   s.ID(),
			ClusterID:            s.spec.ClusterID,
			Endpoint:             *s.EndPoint(),
			CurState:             s.GetCurState(),
			Type:                 PodStatusType,
			TimeStamp:            time.Now(),
			TriggerEventCategory: []common.EventCategory{triggerEventCategory},
		}
	}
}

func (s *PodStatus) HandleEvent(event common.Event) error {
	if !*common.EnableAllAction {
		return nil
	}
	s.Mutex.Lock()
	st := s.state[event.EventCategory()]
	s.Mutex.Unlock()
	if event.GetInsID() == s.spec.ID {
		if sm, exist := s.fsm[event.EventCategory()]; exist {
			return sm.Trigger(s, st, event)
		} else {
			log.Debugf("Skip event %v since category %s not exist", event, event.EventCategory())
		}
	} else {
		log.Debug("Skip event", event, s.spec)
	}
	return nil
}
