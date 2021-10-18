package status

import (
	"fmt"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"strconv"
	"time"
)

type StatusType string

const (
	EngineStatusType StatusType = "EngineStatus"
	PodStatusType    StatusType = "PodStatus"
	ProxyStatusType  StatusType = "ProxyStatus"
)

type StatusEvent struct {
	ID                   string
	ClusterID            string
	Endpoint             common.EndPoint
	CurState             map[common.EventCategory]State
	Type                 StatusType
	LastState            map[common.EventCategory]State
	TimeStamp            time.Time
	TriggerEventCategory []common.EventCategory
}

func (f *StatusEvent) SimpleString() string {
	if f == nil {
		return ""
	}
	return fmt.Sprintf("ID: %v, ClusterID:%v, StatusType:%v, Ep:%v EventCategory: %v, ", f.ID, f.ClusterID, f.Type, f.Endpoint.String(), f.TriggerEventCategory)
}

func (e *StatusEvent) HasTriggerEventCategory(category common.EventCategory) bool {
	for _, ca := range e.TriggerEventCategory {
		if ca == category {
			return true
		}
	}
	return false
}

func StatusID(endpoint common.EndPoint, statusType StatusType) string {
	return string(statusType) + "@" + endpoint.String()
}

type Status interface {
	ID() string
	EndPoint() *common.EndPoint
	TriggerStatusEvent(triggerEventCategory common.EventCategory)
	Stop() error
	Disable()
	Enable()
	Name() string
}

type AlwaysReportTransition struct {
	ReportInterval     int
	BackOffMaxInterval int

	reportInterval int
}

func (d *AlwaysReportTransition) Name() string {
	return "AlwaysReportTransition"
}

func (d *AlwaysReportTransition) Transition(curStatus Status, curState *State, event common.Event, toStateName string, args ...interface{}) {
	curState.Mutex.Lock()

	// 特殊处理
	reason := event.GetValue(common.EventReason)
	if reason != nil {
		curState.Reason = append(curState.Reason, reason.(string))
		if len(curState.Reason) > 3 {
			curState.Reason = curState.Reason[1:]
		}
	}

	if values := event.GetValues(); values != nil {
		for k, v := range values {
			curState.Value[k] = v
		}
	}

	if curState.Name != toStateName {
		if event.EventCategory() != common.EngineMetricsEventCategory &&
			event.EventCategory() != common.EngineConfEventCategory &&
			event.EventCategory() != common.PodMetricsEventCategory {
			log.Infof(" %s/%s from %s to %s action %s", curStatus.EndPoint().String(), event.EventCategory(), curState.Name, toStateName, d.Name())
		}
		curState.Name = toStateName
		curState.ReceivedEvents = 0
		curState.StartTimestamp = event.Timestamp()
	} else {
		curState.ReceivedEvents++
	}

	curState.Mutex.Unlock()

	if d.ReportInterval > 0 {
		if d.BackOffMaxInterval != 0 {
			if d.reportInterval == 0 {
				d.reportInterval = d.ReportInterval
			} else if d.BackOffMaxInterval < d.reportInterval {
				d.reportInterval = d.BackOffMaxInterval
			}
			if curState.ReceivedEvents%d.reportInterval == 0 {
				curStatus.TriggerStatusEvent(event.EventCategory())
				d.reportInterval = d.reportInterval * 2
			}
		} else {
			if curState.ReceivedEvents%d.ReportInterval == 0 {
				curStatus.TriggerStatusEvent(event.EventCategory())
			}
		}
	} else {
		curStatus.TriggerStatusEvent(event.EventCategory())
	}
}

type TimesTransition struct {
	Times int
}

func (d *TimesTransition) Name() string {
	if d.Times == 0 {
		return "DirectTransition"
	} else {
		return strconv.Itoa(d.Times) + "TimesTransition"
	}
}

func (d *TimesTransition) Transition(curStatus Status, curState *State, event common.Event, toStateName string, args ...interface{}) {

	curState.Mutex.Lock()

	// 特殊处理
	reason := event.GetValue(common.EventReason)
	if reason != nil {
		curState.Reason = append(curState.Reason, reason.(string))
		if len(curState.Reason) > 3 {
			curState.Reason = curState.Reason[1:]
		}
	}

	if values := event.GetValues(); values != nil {
		for k, v := range values {
			curState.Value[k] = v
		}
	}

	// 状态切换
	if curState.ReceivedEvents >= d.Times {
		if curState.Name != toStateName {
			if event.EventCategory() != common.EngineMetricsEventCategory &&
				event.EventCategory() != common.EngineConfEventCategory &&
				event.EventCategory() != common.PodMetricsEventCategory {
				log.Infof(" %s/%s from %s to %s action %s", curStatus.EndPoint().String(), event.EventCategory(), curState.Name, toStateName, d.Name())
			}
			curState.Name = toStateName
			curState.ReceivedEvents = 0
			curState.StartTimestamp = event.Timestamp()
		}
	} else {
		curState.ReceivedEvents++
	}

	curState.Mutex.Unlock()

	curStatus.TriggerStatusEvent(event.EventCategory())
}

func DeepCopyStateMap(m map[common.EventCategory]State) map[common.EventCategory]State {
	c := map[common.EventCategory]State{}
	for k, v := range m {
		var reason []string
		for _, r := range v.Reason {
			reason = append(reason, r)
		}
		newV := map[string]interface{}{}
		for vKey, vValue := range v.Value {
			newV[vKey] = vValue
		}
		c[k] = State{
			Name:           v.Name,
			Reason:         reason,
			Value:          newV,
			ReceivedEvents: v.ReceivedEvents,
			StartTimestamp: v.StartTimestamp,
		}
	}
	return c
}
