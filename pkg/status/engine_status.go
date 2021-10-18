package status

import (
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/collector"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/detector"
	"sync"
	"time"
)

const (
	MasterEngineStatus string = "MasterEngineStatus"
)

const (
	EngineStatusAlive   string = "EngineStatusAlive"
	EngineStatusInit    string = "EngineStatusInit"
	EngineStatusRestart string = "EngineStatusRestart"
	EngineStatusLosing  string = "EngineStatusLosing"
	EngineStatusLoss    string = "EngineStatusLoss"
	EngineStatusFailed  string = "EngineStatusFailed"
	EngineStatusDown    string = "EngineStatusDown"

	EngineMetricsConnUnknown = "EngineMetricsConnUnknown"
	EngineMetricsConnNormal  = "EngineMetricsConnNormal"
	EngineMetricsConnFull    = "EngineMetricsConnFull"

	EngineMetricsWaitUnknown = "EngineMetricsWaitUnknown"
	EngineMetricsWait        = "EngineMetricsWait"
	EngineMetricsNoWait      = "EngineMetricsNoWait"

	EngineMetricsReplicaLagUnknown  = "EngineMetricsReplicaLagUnknown"
	EngineMetricsReplicaLagNormal   = "EngineMetricsReplicaLagNormal"
	EngineMetricsReplicaLagDelay    = "EngineMetricsReplicaLagDelay"
	EngineMetricsReplicaLagDelaying = "EngineMetricsReplicaLagDelaying"

	EngineMetricsCollecting = "EngineMetricsCollecting"
	EngineMetricsCollected  = "EngineMetricsCollected"

	EngineConfCollecting = "EngineConfCollecting"
	EngineConfCollected  = "EngineConfCollected"
)

type EngineStatus struct {
	spec           common.InsSpec
	fsm            map[common.EventCategory]*StateMachine
	state          map[common.EventCategory]*State
	statusQueue    chan StatusEvent
	eventCnt       int
	engineFsm      *StateMachine
	engineConnFsm  *StateMachine
	EventProducers []common.EventProducer
	StopFlag       chan bool
	Mutex          sync.Mutex
	StartAt        time.Time
	eventQueue     chan common.EventCategory
}

func NewEngineStatus(spec common.InsSpec, eventProducers []common.EventProducer, queue chan StatusEvent) *EngineStatus {
	if len(eventProducers) == 0 {
		return nil
	}
	engineTrans := []Transition{
		{From: EngineStatusInit, Event: detector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
		{From: EngineStatusInit, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusAlive, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusLosing, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &AlwaysReportTransition{}},
		{From: EngineStatusLosing, Event: detector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
	}

	engineConnFsm := []Transition{
		{From: EngineMetricsConnUnknown, Event: collector.EngineMetricsConnNormalEvent, To: EngineMetricsConnNormal, Action: &TimesTransition{0}},
		{From: EngineMetricsConnUnknown, Event: collector.EngineMetricsConnFullEvent, To: EngineMetricsConnFull, Action: &TimesTransition{0}},
		{From: EngineMetricsConnNormal, Event: collector.EngineMetricsConnFullEvent, To: EngineMetricsConnFull, Action: &TimesTransition{0}},
		{From: EngineMetricsConnNormal, Event: collector.EngineMetricsUnknownEvent, To: EngineMetricsConnUnknown, Action: &TimesTransition{30}},
		{From: EngineMetricsConnFull, Event: collector.EngineMetricsConnNormalEvent, To: EngineMetricsConnNormal, Action: &TimesTransition{10}},
		{From: EngineMetricsConnFull, Event: collector.EngineMetricsUnknownEvent, To: EngineMetricsConnNormal, Action: &TimesTransition{30}},
	}

	replicaDelayFsm := []Transition{
		{From: EngineMetricsReplicaLagUnknown, Event: collector.EngineMetricsReplicaNormal, To: EngineMetricsReplicaLagNormal, Action: &TimesTransition{0}},
		{From: EngineMetricsReplicaLagUnknown, Event: collector.EngineMetricsReplicaDelay, To: EngineMetricsReplicaLagDelaying, Action: &TimesTransition{0}},
		{From: EngineMetricsReplicaLagNormal, Event: collector.EngineMetricsReplicaDelay, To: EngineMetricsReplicaLagDelaying, Action: &TimesTransition{0}},
		{From: EngineMetricsReplicaLagNormal, Event: collector.EngineMetricsReplicaNormal, To: EngineMetricsReplicaLagNormal, Action: &TimesTransition{0}},
		{From: EngineMetricsReplicaLagDelaying, Event: collector.EngineMetricsReplicaNormal, To: EngineMetricsReplicaLagNormal, Action: &TimesTransition{0}},
		{From: EngineMetricsReplicaLagDelaying, Event: collector.EngineMetricsReplicaDelay, To: EngineMetricsReplicaLagDelay, Action: &TimesTransition{int(*common.EngineReplicaDelayMs) / 3000}},
		{From: EngineMetricsReplicaLagDelay, Event: collector.EngineMetricsReplicaNormal, To: EngineMetricsReplicaLagNormal, Action: &TimesTransition{3}},
	}

	engineWaitFsm := []Transition{
		{From: EngineMetricsWaitUnknown, Event: collector.EngineMetricsNoWaitEvent, To: EngineMetricsNoWait, Action: &TimesTransition{0}},
		{From: EngineMetricsWaitUnknown, Event: collector.EngineMetricsWaitEvent, To: EngineMetricsWait, Action: &TimesTransition{0}},
		{From: EngineMetricsNoWait, Event: collector.EngineMetricsWaitEvent, To: EngineMetricsWait, Action: &TimesTransition{0}},
		{From: EngineMetricsNoWait, Event: collector.EngineMetricsUnknownEvent, To: EngineMetricsWaitUnknown, Action: &TimesTransition{3}},
		{From: EngineMetricsWait, Event: collector.EngineMetricsNoWaitEvent, To: EngineMetricsNoWait, Action: &TimesTransition{5}},
		{From: EngineMetricsWait, Event: collector.EngineMetricsUnknownEvent, To: EngineMetricsWaitUnknown, Action: &TimesTransition{5}},
	}

	engineMetricsFsm := []Transition{
		{From: EngineMetricsCollecting, Event: collector.EngineMetricsCollectEvent, To: EngineMetricsCollected, Action: &TimesTransition{0}},
		{From: EngineMetricsCollected, Event: collector.EngineMetricsCollectEvent, To: EngineMetricsCollecting, Action: &TimesTransition{0}},
	}

	engineConfFsm := []Transition{
		{From: EngineConfCollecting, Event: collector.EngineConfCollectEvent, To: EngineConfCollected, Action: &TimesTransition{0}},
		{From: EngineConfCollected, Event: collector.EngineConfCollectEvent, To: EngineConfCollecting, Action: &TimesTransition{0}},
	}

	engineVipFsm := []Transition{
		{From: EngineStatusInit, Event: detector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
		{From: EngineStatusInit, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusAlive, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusLosing, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &AlwaysReportTransition{}},
		{From: EngineStatusLosing, Event: detector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
	}

	enginePodFsm := []Transition{
		{From: EngineStatusInit, Event: detector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
		{From: EngineStatusInit, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusAlive, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusLosing, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &AlwaysReportTransition{}},
		{From: EngineStatusLosing, Event: detector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
	}

	backupAgentFsm := []Transition{
		{From: EngineStatusInit, Event: detector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
		{From: EngineStatusInit, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusAlive, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusLosing, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &AlwaysReportTransition{ReportInterval: 2, BackOffMaxInterval: 30}},
		{From: EngineStatusLosing, Event: detector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
	}

	ueAgentFsm := []Transition{
		{From: EngineStatusInit, Event: detector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
		{From: EngineStatusInit, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusAlive, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusLosing, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &AlwaysReportTransition{ReportInterval: 2, BackOffMaxInterval: 30}},
		{From: EngineStatusLosing, Event: detector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
	}

	engineStatus := &EngineStatus{
		spec: spec,
		fsm: map[common.EventCategory]*StateMachine{
			common.EngineEventCategory:        NewStateMachine(engineTrans...),
			common.EngineConnEventCategory:    NewStateMachine(engineConnFsm...),
			common.EngineDelayEventCategory:   NewStateMachine(replicaDelayFsm...),
			common.EngineWaitEventCategory:    NewStateMachine(engineWaitFsm...),
			common.EngineMetricsEventCategory: NewStateMachine(engineMetricsFsm...),
			common.VipEventCategory:           NewStateMachine(engineVipFsm...),
			common.PodEngineEventCategory:     NewStateMachine(enginePodFsm...),
			common.EngineConfEventCategory:    NewStateMachine(engineConfFsm...),
			common.BackupAgentHBEventCategory: NewStateMachine(backupAgentFsm...),
			common.UeAgentHBEventCategory:     NewStateMachine(ueAgentFsm...),
		},
		state: map[common.EventCategory]*State{
			common.EngineEventCategory:        &State{Name: EngineStatusInit, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.EngineConnEventCategory:    &State{Name: EngineMetricsConnUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.EngineDelayEventCategory:   &State{Name: EngineMetricsReplicaLagUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.EngineWaitEventCategory:    &State{Name: EngineMetricsWaitUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.VipEventCategory:           &State{Name: EngineStatusInit, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.PodEngineEventCategory:     &State{Name: EngineStatusInit, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.EngineMetricsEventCategory: &State{Name: EngineMetricsCollecting, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.EngineConfEventCategory:    &State{Name: EngineConfCollecting, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.BackupAgentHBEventCategory: &State{Name: EngineStatusInit, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
			common.UeAgentHBEventCategory:     &State{Name: EngineStatusInit, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		},
		statusQueue:    queue,
		eventCnt:       0,
		EventProducers: eventProducers,
		engineFsm:      NewStateMachine(engineTrans...),
		engineConnFsm:  NewStateMachine(engineConnFsm...),
		StopFlag:       make(chan bool),
		StartAt:        time.Now(),
		eventQueue:     make(chan common.EventCategory, 128),
	}

	for _, eventProducer := range eventProducers {
		eventProducer.RegisterEventCallback(engineStatus)
		eventProducer.Start()
	}

	go engineStatus.StatusEventLoop()

	return engineStatus
}

func (s *EngineStatus) Disable() {
	s.StopFlag <- true
}

func (s *EngineStatus) Enable() {
	s.Mutex.Lock()

	s.state = map[common.EventCategory]*State{
		common.EngineEventCategory:        &State{Name: EngineStatusInit, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		common.EngineConnEventCategory:    &State{Name: EngineMetricsConnUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		common.EngineDelayEventCategory:   &State{Name: EngineMetricsReplicaLagUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		common.EngineWaitEventCategory:    &State{Name: EngineMetricsWaitUnknown, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		common.VipEventCategory:           &State{Name: EngineStatusInit, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		common.PodEngineEventCategory:     &State{Name: EngineStatusInit, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		common.EngineMetricsEventCategory: &State{Name: EngineMetricsCollecting, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		common.EngineConfEventCategory:    &State{Name: EngineConfCollecting, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		common.BackupAgentHBEventCategory: &State{Name: EngineStatusInit, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		common.UeAgentHBEventCategory:     &State{Name: EngineStatusInit, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
	}

	s.Mutex.Unlock()

	s.StartAt = time.Now()
	for _, producer := range s.EventProducers {
		producer.ReStart()
	}

	go s.StatusEventLoop()
}

func (s *EngineStatus) Stop() error {
	s.StopFlag <- true
	for _, producer := range s.EventProducers {
		producer.Stop()
	}
	log.Infof("EngineStatus %s stop.", s.spec.String())
	return nil
}

func (s *EngineStatus) Name() string {
	return "EngineStatus"
}

func (s *EngineStatus) ID() string {
	return s.spec.ID
}

func (s *EngineStatus) EndPoint() *common.EndPoint {
	return &s.spec.Endpoint
}

func (s *EngineStatus) GetCurState() map[common.EventCategory]State {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	curStates := map[common.EventCategory]State{}
	for k, v := range s.state {
		v.Mutex.Lock()

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

		v.Mutex.Unlock()
	}
	return curStates
}

func (s *EngineStatus) StatusEventLoop() {
	ticker := time.NewTicker(time.Millisecond * 500)
	var eventList []common.EventCategory
	for {
		select {
		case ev := <-s.eventQueue:
			eventList = append(eventList, ev)
		case <-ticker.C:
			if len(eventList) > 0 {
				s.statusQueue <- StatusEvent{
					ID:                   s.ID(),
					ClusterID:            s.spec.ClusterID,
					Endpoint:             *s.EndPoint(),
					CurState:             s.GetCurState(),
					Type:                 EngineStatusType,
					TimeStamp:            time.Now(),
					TriggerEventCategory: eventList,
				}
				eventList = []common.EventCategory{}
			}
		case <-s.StopFlag:
			log.Infof("EngineStatus %v event loop stopped", s)
			ticker.Stop()
			return
		}
	}

}

func (s *EngineStatus) TriggerStatusEvent(triggerEventCategory common.EventCategory) {
	s.eventQueue <- triggerEventCategory
}

func (s *EngineStatus) HandleEvent(event common.Event) error {
	if !*common.EnableAllAction &&
		event.Name() != detector.EngineAliveEvent &&
		event.Name() != collector.EngineConfCollectEvent &&
		event.Name() != collector.EngineMetricsReplicaNormal {
		return nil
	}
	s.Mutex.Lock()
	st := s.state[event.EventCategory()]
	s.Mutex.Unlock()
	if event.GetInsID() == s.spec.ID {
		if sm, exist := s.fsm[event.EventCategory()]; exist {
			return sm.Trigger(s, st, event)
		}
	} else {
		log.Debug("Skip event", event, s.spec)
	}
	return nil
}
