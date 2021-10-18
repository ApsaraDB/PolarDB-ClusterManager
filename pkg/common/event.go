package common

import (
	"fmt"
	"time"
)

type EventCategory string

const (
	EngineEventCategory        EventCategory = "EngineEventCategory"
	VipEventCategory           EventCategory = "VipEventCategory"
	PodEngineEventCategory     EventCategory = "PodEngineEventCategory"
	BackupAgentHBEventCategory EventCategory = "BackupAgentHBEventCategory"
	UeAgentHBEventCategory     EventCategory = "UeAgentHBEventCategory"
	CmHBEventCategory          EventCategory = "CmHBEventCategory"

	PodCpuEventCategory       EventCategory = "PodCpuEventCategory"
	MetricsEventCategory      EventCategory = "MetricsEventCategory"
	PodEventCategory          EventCategory = "PodEventCategory"
	StorageUsageEventCategory EventCategory = "StorageUsageEventCategory"
	PodNetworkEventCategory   EventCategory = "PodNetworkEventCategory"
	PodMetricsEventCategory   EventCategory = "PodMetricsEventCategory"

	EngineConnEventCategory    EventCategory = "EngineConnEventCategory"
	EngineWaitEventCategory    EventCategory = "EngineWaitEventCategory"
	EngineMetricsEventCategory EventCategory = "EngineMetricsEventCategory"
	EngineDelayEventCategory   EventCategory = "EngineDelayEventCategory"
	EngineConfEventCategory    EventCategory = "EngineConfEventCategory"

	EventReason                   string = "EventReason"
	EventTimestamp                string = "EventTimestamp"
	EventWriteDelayTime           string = "EventWriteDelayTime"
	EventWriteDelaySize           string = "EventWriteDelaySize"
	EventReplayDelayTime          string = "EventReplayDelayTime"
	EventReplayDelaySize          string = "EventReplayDelaySize"
	EventDelayTime                string = "EventDelayTime"
	EventDelaySize                string = "EventDelaySize"
	EventReplicaRestartSize       string = "EventReplicaRestartSize"
	EventCopyBufferUsage          string = "EventCopyBufferUsage"
	EventSharedBufferUsage        string = "EventSharedBufferUsage"
	EventMaxRecoverySize          string = "MaxRecoverySize"
	EventRoReplayBgReplayDiffSize string = "EventRoReplayBgReplayDiffSize"
	EventServerStatus             string = "EventServerStatus"
	EventPfsThroughput            string = "EventPfsThroughput"
	EventCommitIndex              string = "EventCommitIndex"
	EventPaxosRole                string = "EventPaxosRole"
	EventStandbyReplayRate        string = "EventStandbyReplayRate"
	EventStandbyReplayData        string = "EventStandbyReplayData"
	EventStandbyRTO               string = "EventStandbyRTO"

	EventFileConf string = "EventFileConf"

	EventWaitEventType string = "EventWaitEventType"
	EventWaitEvent     string = "EventWaitEvent"
	EventOnlinePromote string = "EventOnlinePromote"
	EventPersistSlot   string = "EventPersistSlot"

	MetricsCpuStatus       string = "cpu_status"
	MetricsOnlinePromote   string = "enable_online_promote"
	MetricsRecoverySize    string = "recovery_size"
	MetricsWaitEvent       string = "wait_event"
	MetricsEngineState     string = "engine_state"
	MetricsTps             string = "tps"
	MetricsCpuUsage        string = "cpu_usage"
	MetricsMemUsage        string = "mem_usage"
	MetricsConn            string = "connections"
	MetricsReadIOPS        string = "read_iops"
	MetricsWriteIOPS       string = "write_iops"
	MetricsReadThroughPut  string = "read_throughput"
	MetricsWriteThroughPut string = "write_throughput"
	MetricsPaxosRole       string = "paxos_role"
	MetricsDetectError     string = "detect_error"
	MetricsLastDetectError string = "last_detect_error"
	MetricsPodStatus       string = "pod_status"
	MetricsPodEngineStatus string = "pod_engine_status"
	MetricsRtoStart        string = "rto_start"
	MetricsDecisionTime    string = "time"
)

type Event interface {
	Name() string
	EventCategory() EventCategory
	GetInsID() string
	GetEndpoint() EndPoint
	Timestamp() time.Time
	GetValue(key string) interface{}
	GetValues() map[string]interface{}
}

func EnvSimpleName(env Event) string {
	if env == nil {
		return "event is null"
	}
	return fmt.Sprintf("Name: %v, EventCategory: %v, InsId: %v, Ep:%+v, timestamp:%v", env.Name(), env.EventCategory(), env.GetInsID(), env.GetEndpoint(), env.Timestamp())
}

type EventHandler interface {
	HandleEvent(event Event) error
	Name() string
}

type EventProducer interface {
	ID() string
	ClusterID() string
	EndPoint() EndPoint
	Start() error
	Stop() error
	ReStart() error
	RegisterEventCallback(callback EventHandler) error
}

type BaseEvent struct {
	EvName      string
	InsID       string
	Ep          EndPoint
	EvTimestamp time.Time
	Values      map[string]interface{}
}

func (e *BaseEvent) GetValue(key string) interface{} {
	if v, exist := e.Values[key]; exist {
		return v
	}

	return nil
}

func (e *BaseEvent) GetValues() map[string]interface{} {
	return e.Values
}

func (e *BaseEvent) GetEndpoint() EndPoint {
	return e.Ep
}

func (e *BaseEvent) GetInsID() string {
	return e.InsID
}

func (e *BaseEvent) Timestamp() time.Time {
	return e.EvTimestamp
}

func (e *BaseEvent) Name() string {
	return e.EvName
}

const EngineTimeout string = "timeout"

const EngineConnRefused string = "connection refused"

const EngineShutdown string = "the database system is shutting down"

const EngineStart string = "the database system is starting up"

const EngineReadOnly string = "read-only"

const EngineRecovery string = "the database system is in recovery mode"

const EnginePXMoreSegs string = "failed to acquire resources on one or more segments"

const EngineConnExceed string = "connections has exceeded"

const EngineConnReset string = "connection reset by peer"

const EngineUnreachable string = "network is unreachable"

const EngineNoRouteToHost string = "no route to host"

const EngineWrapAroundDataLoss string = "database is not accepting commands to avoid wraparound data loss"

const EngineDetectorEventProducer = "EngineDetectorEventProducer"

var DetailFailReason = []string{EngineTimeout, EngineConnReset, EngineConnRefused, EngineWrapAroundDataLoss,
	EngineShutdown, EngineStart, EngineReadOnly, EngineRecovery, EngineConnExceed, EngineUnreachable, EnginePXMoreSegs, EngineNoRouteToHost}
