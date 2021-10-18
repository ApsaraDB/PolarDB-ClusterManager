package status

import (
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/detector"
	"sync"
	"time"
)

const (
	ProxyStatusAlive  string = "ProxyStatusAlive"
	ProxyStatusInit   string = "ProxyStatusInit"
	ProxyStatusLosing string = "ProxyStatusLosing"
	ProxyStatusLoss   string = "ProxyStatusLoss"
	ProxyStatusFailed string = "ProxyStatusFailed"
	ProxyStatusDown   string = "ProxyStatusDown"
)

type ProxyStatus struct {
	spec           common.InsSpec
	fsm            map[common.EventCategory]*StateMachine
	state          map[common.EventCategory]*State
	statusQueue    chan StatusEvent
	eventCnt       int
	EventProducers []common.EventProducer
	StopFlag       bool
	Mutex          sync.Mutex
}

func NewProxyStatus(spec common.InsSpec, eventProducers []common.EventProducer, queue chan StatusEvent) *ProxyStatus {
	if len(eventProducers) == 0 {
		return nil
	}
	engineTrans := []Transition{
		{From: ProxyStatusInit, Event: detector.EngineAliveEvent, To: ProxyStatusAlive, Action: &TimesTransition{0}},
		{From: ProxyStatusInit, Event: detector.EngineFailEvent, To: ProxyStatusLosing, Action: &TimesTransition{10}},
		{From: ProxyStatusAlive, Event: detector.EngineFailEvent, To: ProxyStatusLosing, Action: &TimesTransition{0}},
		{From: ProxyStatusLosing, Event: detector.EngineFailEvent, To: ProxyStatusLoss, Action: &TimesTransition{10}},
		{From: ProxyStatusLosing, Event: detector.EngineAliveEvent, To: ProxyStatusAlive, Action: &TimesTransition{0}},
		{From: ProxyStatusLoss, Event: detector.EngineFailEvent, To: ProxyStatusFailed, Action: &TimesTransition{10}},
		{From: ProxyStatusLoss, Event: detector.EngineAliveEvent, To: ProxyStatusAlive, Action: &TimesTransition{0}},
		{From: ProxyStatusFailed, Event: detector.EngineFailEvent, To: ProxyStatusDown, Action: &TimesTransition{10}},
		{From: ProxyStatusFailed, Event: detector.EngineAliveEvent, To: ProxyStatusAlive, Action: &TimesTransition{0}},
		{From: ProxyStatusDown, Event: detector.EngineAliveEvent, To: ProxyStatusAlive, Action: &TimesTransition{10}},
	}

	ueAgentFsm := []Transition{
		{From: EngineStatusInit, Event: detector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
		{From: EngineStatusInit, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusAlive, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &TimesTransition{0}},
		{From: EngineStatusLosing, Event: detector.EngineFailEvent, To: EngineStatusLosing, Action: &AlwaysReportTransition{ReportInterval: 2, BackOffMaxInterval: 30}},
		{From: EngineStatusLosing, Event: detector.EngineAliveEvent, To: EngineStatusAlive, Action: &TimesTransition{0}},
	}

	proxyStatus := &ProxyStatus{
		spec: spec,
		fsm: map[common.EventCategory]*StateMachine{
			common.EngineEventCategory:    NewStateMachine(engineTrans...),
			common.UeAgentHBEventCategory: NewStateMachine(ueAgentFsm...),
		},
		state: map[common.EventCategory]*State{
			common.EngineEventCategory:    &State{Name: ProxyStatusInit, Value: make(map[string]interface{})},
			common.UeAgentHBEventCategory: &State{Name: EngineStatusInit, StartTimestamp: time.Now(), Value: make(map[string]interface{})},
		},
		statusQueue:    queue,
		eventCnt:       0,
		EventProducers: eventProducers,
		StopFlag:       false,
	}

	for _, eventProducer := range eventProducers {
		eventProducer.RegisterEventCallback(proxyStatus)
		eventProducer.Start()
	}

	return proxyStatus
}

func (s *ProxyStatus) Disable() {
}

func (s *ProxyStatus) Enable() {
}

func (s *ProxyStatus) Stop() error {
	for _, producer := range s.EventProducers {
		producer.Stop()
	}
	s.StopFlag = true
	log.Infof("ProxyStatus %v stop.", s)
	return nil
}

func (s *ProxyStatus) ID() string {
	return s.spec.ID
}

func (s *ProxyStatus) Name() string {
	return "ProxyStatus"
}

func (s *ProxyStatus) EndPoint() *common.EndPoint {
	return &s.spec.Endpoint
}

func (s *ProxyStatus) GetCurState() map[common.EventCategory]State {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	curState := map[common.EventCategory]State{}
	for k, v := range s.state {
		curState[k] = *v
	}
	return curState
}

func (s *ProxyStatus) TriggerStatusEvent(triggerEventCategory common.EventCategory) {
	if s.StopFlag {
		log.Warnf("Received unexpected status event %v", s)
	} else {
		s.statusQueue <- StatusEvent{
			ID:                   s.ID(),
			ClusterID:            s.spec.ClusterID,
			Endpoint:             *s.EndPoint(),
			CurState:             s.GetCurState(),
			Type:                 ProxyStatusType,
			TimeStamp:            time.Now(),
			TriggerEventCategory: []common.EventCategory{triggerEventCategory},
		}
	}
}

func (s *ProxyStatus) HandleEvent(event common.Event) error {
	if !*common.EnableAllAction && event.Name() != detector.EngineAliveEvent {
		return nil
	}
	s.Mutex.Lock()
	st := s.state[event.EventCategory()]
	s.Mutex.Unlock()
	if sm, exist := s.fsm[event.EventCategory()]; exist {
		return sm.Trigger(s, st, event)
	} else {
		log.Debug("Skip event", event)
	}
	return nil
}
