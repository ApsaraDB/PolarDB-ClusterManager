package common

import (
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"regexp"
	"strings"
	"time"
)

type WorkMode string

const (
	PLUGIN_TOPOLOGY_EVENT string = "PluginTopologyEvent"
)

const (
	DUPLICATE_OBJECT_ERROR string = "42710"
)

const (
	POLAR_CLOUD   WorkMode = "CloudMode"
	POLAR_BOX_ONE WorkMode = "PolarBoxOne"
	POLAR_BOX_MUL WorkMode = "PolarBoxMul"
	POLAR_PAAS_MS WorkMode = "PolarPaasMS"
	POLAR_PURE    WorkMode = "PolarPure"

	LEADER       string = "Leader"
	FOLLOWER     string = "Follower"
	LeaderRole   string = "Leader"
	FollowerRole string = "Follower"

	PolarStore string = "PolarStore"
	LocalStore string = "LocalStore"
)

type EngineType string

const (
	RwEngine      EngineType = "RwEngine"
	RoEngine      EngineType = "RoEngine"
	StandbyEngine EngineType = "StandbyEngine"

	Master         EngineType = "Master"
	Standby        EngineType = "Standby"
	Readonly       EngineType = "Readonly"
	Leader         EngineType = "Leader"
	Follower       EngineType = "Follower"
	Learner        EngineType = "Learner"
	DataMax        EngineType = "DataMax"
	RW             EngineType = "RW"
	RO             EngineType = "RO"
	Proxy          EngineType = "Proxy"
	ClusterManager EngineType = "ClusterManager"
)

const (
	RESPONSE_OK             = "{\"code\":200}"
	RESPONSE_INTERNAL_ERROR = "{\"code\":500}"
)

const (
	// cluster manager phase
	InitPhase       = "InitPhase"
	RunningPhase    = "RunningPhase"
	StoppedPhase    = "StoppedPhase"
	DisableAllPhase = "DisableAllPhase"
	SwitchoverPhase = "SwitchoverPhase"
	SwitchingPhase  = "SwitchingPhase"
	InsChangePhase  = "InsChangePhase"

	// engine phase
	EnginePhasePending  string = "PENDING"
	EnginePhaseStarting string = "STARTING"
	EnginePhaseRunning  string = "RUNNING"
	EnginePhaseStopping string = "STOPPING"
	EnginePhaseStopped  string = "STOPPED"
	EnginePhaseFailed   string = "FAILED"

	// switch type
	MasterReplicaSwitch  string = "MasterReplicaSwitch"
	MasterStandbySwitch  string = "MasterStandbySwitch"
	StandbyReplicaSwitch string = "StandbyReplicaSwitch"

	SyncStatusTag string = "SyncStatusTag"

	SyncStatusSync   string = "SYNC"
	SyncStatusUnSync string = "UNSYNC"

	// alarm operation
	AddAlarm       string = "AddAlarm"
	RemoveAlarm    string = "RemoveAlarm"
	RemoveInsAlarm string = "RemoveInsAlarm"

	// alarm type
	AlarmRoFailedType     string = "AlarmRoFailedType"
	AlarmActionExecFailed string = "AlarmActionExecFailed"
	AlarmRoDelay          string = "AlarmRoDelay"
	AlarmCrash            string = "AlarmCrash"

	// alarm event type
	AlarmSmsEventType   string = "k8s_custins_abnormal_cm"
	AlarmPhoneEventType string = "k8s_custins_abnormal_important"

	// config map postfix
	FlagConfigMapPostfix    string = "-cmflag"
	CmConfigMapPostfix      string = "-cmconfig"
	CmLeaderElectionPostfix string = "-cmleader"
	AlarmConfigMapPostfix   string = "-alarm"

	MaxReplicaDelayTime int = 99999
	MaxReplicaDelaySize int = 999999999

	ServerStatusOff              string = "off"
	ServerStatusOn               string = "on"
	NodeClientNetworkUnavailable string = "NodeClientNetworkUnavailable"
	PlugInStorageUnavailable     string = "PlugInStorageUnavailable"

	DefaultDatabase string = "polardb_admin"

	SharedStorageCluster string = "SharedStorageCluster"
	SharedNothingCluster string = "SharedNothingCluster"
	PaxosCluster         string = "PaxosCluster"
)

type ApiResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

var StopFlag = false

type MonitorItem struct {
	CollectSQL string     `json:"collect_sql"`
	Name       string     `json:"name"`
	EngineType EngineType `json:"engine_type"`
	IsAlarm    bool       `json:"is_alarm,omitempty"`
	Msg        string     `json:"msg,omitempty"`
}

type SQLMonitorConfig struct {
	Items []MonitorItem `json:"items"`
}

type AlarmMsg struct {
	EventType   string            `json:"eventType"`
	Message     string            `json:"message"`
	ClusterName string            `json:"dbCluster"`
	Tags        map[string]string `json:"tags"`
}

type AlarmInfo struct {
	AlarmMsg

	Spec    InsSpec   `json:"spec,omitempty"`
	Type    string    `json:"type,omitempty"`
	StartAt time.Time `json:"start_at,omitempty"`
}

type Alarms struct {
	Alarm map[string]map[string]AlarmInfo
}

func (a *AlarmInfo) String() string {
	if buf, err := json.Marshal(a); err != nil {
		log.Warnf("Failed to Marshal %s")
		return ""
	} else {
		return string(buf)
	}

}

type InsPrevSpec struct {
	PodName  string   `json:"podName,omitempty"`
	PodID    string   `json:"podID,omitempty"`
	Endpoint EndPoint `json:"endpoint,omitempty"`
}

func (in *InsPrevSpec) DeepCopy() *InsPrevSpec {
	if in == nil {
		return nil
	}
	out := &InsPrevSpec{}
	out.PodName = in.PodName
	out.PodID = in.PodID
	out.Endpoint = in.Endpoint
	return out
}

type InsSpec struct {
	// add by add ins api
	ID        string `json:"id,omitempty"`
	ClusterID string `json:"clusterID,omitempty"`
	CustID    string `json:"custID,omitempty"`
	// used by node driver
	UseNodeDriver       bool   `json:"use_node_driver,omitempty"`
	NodeDriverMemberID  string `json:"NodeDriverMemberID,omitempty"`
	NodeDriverClusterID string `json:"NodeDriverClusterID,omitempty"`

	Port           string   `json:"port,omitempty"`
	PodName        string   `json:"podName,omitempty"`
	DataPath       string   `json:"data_path,omitempty"`
	SwitchPriority int      `json:"switch_priority"`
	DBInstanceName string   `json:"dbInstanceName,omitempty"`
	Sync           SyncType `json:"sync,omitempty"`

	IsDeleted bool `json:"isDeleted,omitempty"`
	IsStandby bool `json:"is_standby,omitempty"`

	IsDataMax   bool   `json:"is_data_max"`
	ClusterType string `json:"cluster_type"`

	// generated by rule
	EngineUser     string `json:"engineUser,omitempty"`
	AuroraPassword string `json:"aurora_password"`

	// get from k8s
	Endpoint     EndPoint `json:"endpoint,omitempty"`
	HostName     string   `json:"hostName,omitempty"`
	PodID        string   `json:"podID,omitempty"`
	MaxMemSizeMB int64    `json:"maxMemSizeMB,omitempty"`

	// get from engine
	RecoverySizeMB int64 `json:"recoverySizeMB,omitempty"`

	// PolarBox
	VipEndpoint EndPoint `json:"vip_endpoint,omitempty"`

	//上一次的Pod信息
	PrevSpec *InsPrevSpec `json:"prev_spec,omitempty"`
}

func (in *InsSpec) DeepCopyInto(out *InsSpec) {
	*out = *in
	if in.PrevSpec != nil {
		out.PrevSpec = in.PrevSpec.DeepCopy()
	} else {
		out.PrevSpec = nil
	}

}

func (i *InsSpec) SimpleName() string {
	if i == nil {
		return ""
	}
	return fmt.Sprintf("CustID=%v,Id=%v,PodName=%v", i.CustID, i.ID, i.PodName)
}

func (s *InsSpec) IsRPMDeploy() bool {
	return s.DataPath != ""
}

func (s *InsSpec) IsDockerDeploy() bool {
	return s.PodName != ""
}

func (s *InsSpec) IsNodeDriverDeploy() bool {
	return s.NodeDriverClusterID != ""
}

type ProxySpec struct {
	InstanceSpec map[EndPoint]*InsSpec
}

type ProxyMeta struct {
	InstanceSpec map[string]*InsSpec
}

type ClusterSpec struct {
	SubCluster          map[string]*SubClusterSpec `json:"sub_cluster"`
	InitMasterClusterID string                     `json:"init_master_cluster_id"`
	ReadOnly            bool                       `json:"read_only"`
	Stopped             bool                       `json:"stopped"`
	ProxySpec           map[EndPoint]*InsSpec
}

type SubClusterSpec struct {
	ClusterID    string                `json:"clusterID,omitempty"`
	InitRwID     string                `json:"initRwID"`
	InstanceSpec map[EndPoint]*InsSpec `json:"instanceSpec"`
}

type EndPoint struct {
	Host string `json:"host,omitempty"`
	Port string `json:"port,omitempty"`
}

type SwitchAction struct {
	Manual       bool      `json:"manual,omitempty"`
	Force        bool      `json:"force,omitempty"`
	RPO          int       `json:"rpo,omitempty"`
	RTO          int       `json:"rto,omitempty"`
	OldRw        InsSpec   `json:"oldRw,omitempty"`
	OldRo        []InsSpec `json:"oldRo,omitempty"`
	NewRw        InsSpec   `json:"newRw,omitempty"`
	OldStandby   []InsSpec `json:"oldStandby,omitempty"`
	Master       InsSpec   `json:"master,omitempty"`
	Type         string    `json:"type,omitempty"`
	RecoverySize int64     `json:"recoverySize,omitempty"`
}

type SwitchTask struct {
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	Action    SwitchAction `json:"action,omitempty"`
	Phase     string       `json:"phase,omitempty"`
	FinishAt  *metav1.Time `json:"finishAt,omitempty"`
	Reason    HaReason     `json:"haReason,omitempty"`
}

type VipSpec struct {
	Vip       string `json:"vip"`
	Interface string `json:"interface"`
	Mask      string `json:"mask"`
	Endpoint  string `json:"endpoint"`
}

type ClusterManagerConfig struct {
	Port        int        `json:"Port,omitempty"`
	Phase       string     `json:"phase,omitempty"`
	RunningTask SwitchTask `json:"runningTask,omitempty"`
	Spec        string     `json:"spec,omitempty"`
	Meta        string     `json:"meta,omitempty"`
	Vip         []VipSpec  `json:"vip,omitempty"`
	ProxyMeta   string     `json:"proxy_meta,omitempty"`
	CmConf      string     `json:"cm_conf"`
}

type SwitchStep struct {
	Name        string `json:"name"`
	TimeElapsed string `json:"time_elapsed"`
	Timestamp   string `json:"timestamp"`
	Log         string `json:"log"`
}

type HaReason struct {
	Reason        string                 `json:"reason"`
	DecisionTime  string                 `json:"decision_time,omitempty"`
	RtoStart      time.Time              `json:"rto_start,omitempty"`
	ActionStartAt time.Time              `json:"action_start_at,omitempty"`
	Metrics       map[string]interface{} `json:"metrics,omitempty"`
	DecisionTree  []string               `json:"decision_tree,omitempty"`
}

func (r *HaReason) String() string {
	res, err := json.Marshal(*r)
	if err != nil {
		return fmt.Sprintf("Failed to Marshal %v err %s", *r, err.Error())
	}
	return string(res)
}

type SwitchEvent struct {
	UUID             string        `json:"uuid"`
	Action           SwitchAction  `json:"action"`
	StartAt          time.Time     `json:"start_at"`
	FinishAt         time.Time     `json:"finish_at"`
	Steps            []SwitchStep  `json:"steps"`
	Reason           HaReason      `json:"reason"`
	RTO              time.Duration `json:"rto"`
	SwitchTime       time.Duration `json:"switch_time"`
	AffectedSessions int           `json:"affected_sessions"`
}

type SwitchItem struct {
	SwitchCauseCode  string    `json:"SwitchCauseCode"`
	TotalSessions    int       `json:"TotalSessions"`
	SwitchFinishTime time.Time `json:"SwitchFinishTime"`
	SwitchStartTime  time.Time `json:"SwitchStartTime"`
	SwitchId         string    `json:"SwitchId"`
	AffectedSessions int       `json:"AffectedSessions"`
}

const (
	SwitchOver                = "SwitchOver"
	FailoverForConnectFailure = "FailoverForConnectFailure"
	FailoverForTimeoutFailure = "FailoverForTimeoutFailure"
	FailoverForOthers         = "FailoverForOthers"
)

func (e *SwitchEvent) ToSwitchItem() SwitchItem {
	item := SwitchItem{
		SwitchStartTime:  e.StartAt,
		SwitchFinishTime: e.FinishAt,
		SwitchId:         e.UUID,
		TotalSessions:    e.AffectedSessions,
		AffectedSessions: e.AffectedSessions,
	}

	if e.Action.Manual {
		item.SwitchCauseCode = SwitchOver
	} else {
		if strings.Contains(e.Reason.Reason, EngineConnRefused) {
			item.SwitchCauseCode = FailoverForConnectFailure
		} else if strings.Contains(e.Reason.Reason, EngineTimeout) {
			item.SwitchCauseCode = FailoverForTimeoutFailure
		} else {
			item.SwitchCauseCode = FailoverForOthers
		}
	}

	return item
}

type SwitchLog struct {
	LogItems   []SwitchItem  `json:"LogItems"`
	UUID       string        `json:"uuid"`
	Action     SwitchAction  `json:"action"`
	StartAt    time.Time     `json:"start_at"`
	Steps      []SwitchStep  `json:"steps"`
	Reason     HaReason      `json:"reason"`
	RTO        time.Duration `json:"rto"`
	SwitchTime time.Duration `json:"switch_time"`
}

func NewClusterSpec() *ClusterSpec {
	return &ClusterSpec{
		SubCluster: make(map[string]*SubClusterSpec),
		ProxySpec:  make(map[EndPoint]*InsSpec),
	}
}

func (a *SwitchAction) String() string {
	return a.OldRw.String() + " to " + a.NewRw.String()
}

func (i *InsSpec) String() string {
	return i.ClusterID + "/" + i.Endpoint.String() + "/" + i.PodName + "/" + i.HostName
}

func (e *EndPoint) String() string {
	return e.Host + ":" + e.Port
}

func (e *EndPoint) IsDefault() bool {
	return e.Host == "" && e.Port == ""
}

func NewEndPointWithPanic(ep string) *EndPoint {
	match, _ := regexp.MatchString(`[\d\.]{3}\d\:\d`, ep)
	if !match {
		panic(nil)
	}
	s := strings.Split(ep, ":")

	return &EndPoint{
		Host: s[0],
		Port: s[1],
	}
}

func NewEndPoint(ep string) (*EndPoint, error) {
	match, _ := regexp.MatchString(`[\d\.]{3}\d\:\d`, ep)
	if !match {
		return nil, errors.Errorf("Invalid ep %s", ep)
	}
	s := strings.Split(ep, ":")

	return &EndPoint{
		Host: s[0],
		Port: s[1],
	}, nil
}

type SyncType string

const (
	SYNC  SyncType = "SYNC"
	ASYNC SyncType = "ASYNC"
)

func (in *SwitchAction) DeepCopyInto(out *SwitchAction) {
	*out = *in
	out.OldRw = in.OldRw
	if in.OldRo != nil {
		in, out := &in.OldRo, &out.OldRo
		*out = make([]InsSpec, len(*in))
		copy(*out, *in)
	}
	out.NewRw = in.NewRw
	if in.OldStandby != nil {
		in, out := &in.OldStandby, &out.OldStandby
		*out = make([]InsSpec, len(*in))
		copy(*out, *in)
	}
	out.Master = in.Master
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SwitchAction.
func (in *SwitchAction) DeepCopy() *SwitchAction {
	if in == nil {
		return nil
	}
	out := new(SwitchAction)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *SwitchTask) DeepCopyInto(out *SwitchTask) {
	*out = *in
	if in.StartedAt != nil {
		in, out := &in.StartedAt, &out.StartedAt
		*out = (*in).DeepCopy()
	}
	in.Action.DeepCopyInto(&out.Action)
	if in.FinishAt != nil {
		in, out := &in.FinishAt, &out.FinishAt
		*out = (*in).DeepCopy()
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SwitchTask.
func (in *SwitchTask) DeepCopy() *SwitchTask {
	if in == nil {
		return nil
	}
	out := new(SwitchTask)
	in.DeepCopyInto(out)
	return out
}

type ClusterSpecMeta struct {
	SubCluster          map[string]*SubClusterSpecMeta `json:"sub_cluster"`
	InitMasterClusterID string                         `json:"init_master_cluster_id"`
	ReadOnly            bool                           `json:"read_only"`
	Stopped             bool                           `json:"stopped"`
}

type SubClusterSpecMeta struct {
	InstanceSpec map[string]*InsSpec `json:"instanceSpec"`
	ClusterID    string              `json:"clusterID,omitempty"`
	InitRwID     string              `json:"initRwID"`
	ClusterType  string              `json:"cluster_type"`
}

type ClusterStatusMeta struct {
	SubCluster      map[string]*SubClusterStatusMeta `json:"sub_cluster"`
	MasterClusterID string                           `json:"master_cluster_id"`
	Phase           string                           `json:"phase"`
	OrphanCluster   map[string]*SubClusterStatusMeta `json:"orphan_cluster"`
}

type FileSetting struct {
	Name                 string
	FileSetting          string
	UnAppliedFileSetting string
	UserSetting          string
	Context              string
	Sourcefile           string
	Sourceline           int
	Error                string
	Applied              bool
	Unit                 string
	Vartype              string
	Reset                bool
}

type VersionFileConf struct {
	Timestamp time.Time              `json:"timestamp"`
	Conf      map[string]FileSetting `json:"conf"`
}

type ClusterFileConf struct {
	Conf     map[string]FileSetting `json:"conf"`
	Versions []VersionFileConf      `json:"versions"`
}

type TagMap struct {
	Tags map[string]string `json:"tags"`
}

type SubClusterStatusMeta struct {
	EnginePhase        map[string]string    `json:"engine_phase"`
	EnginePhaseStartAt map[string]time.Time `json:"engine_phase_start_at"`
	RwEndpoint         EndPoint             `json:"rw_endpoint"`
	EngineTag          map[string]*TagMap   `json:"engine_tag"`
	ClusterType        string               `json:"cluster_type"`
	ClusterTag         TagMap               `json:"cluster_tag"`
}

func (in *SubClusterStatusMeta) DeepCopy() *SubClusterStatusMeta {
	if in == nil {
		return nil
	}

	jsonStr, err := json.Marshal(in)

	if err != nil {
		return nil
	}

	out := &SubClusterStatusMeta{}
	oErr := json.Unmarshal(jsonStr, out)
	if oErr != nil {
		return nil
	}
	return out
}

type SpecEvent struct {
	Times int
}

const InternalMarkSQL = "/* rds internal mark */"

const LockReadOnlySQL = InternalMarkSQL + "ALTER SYSTEM set polar_force_trans_ro_non_sup = on"

const UnlockReadOnlySQL = InternalMarkSQL + "alter system reset polar_force_trans_ro_non_sup"

const ReloadConfSQL = InternalMarkSQL + "select pg_reload_conf()"

const ShowSynchronousStandbyNames = InternalMarkSQL + "show synchronous_standby_names"

const TerminateBackendSQL = InternalMarkSQL +
	"select pg_terminate_backend(pid) from pg_stat_activity where client_addr is not null and usename not in ('replicator','aurora');"

const PolarDBRwHealthCheckSQL string = InternalMarkSQL +
	`
		set idle_in_transaction_session_timeout = 30000;
		do $$
			begin
			update ha_health_check set id = extract(epoch from now()) where type = 1;
			if not found then
				begin
					insert into ha_health_check(type, id) values (1, extract(epoch from now()));
				exception when unique_violation then
				end;
			end if;
		exception when undefined_table then
			create table if not exists ha_health_check(type int, id bigint, primary key(type));
			begin
				insert into ha_health_check(type, id) values (1, extract(epoch from now()));
			exception when unique_violation then
			end;
		end $$;
		select id from ha_health_check;`

const PolarDBRoHealthCheckSQL string = InternalMarkSQL + "select id from ha_health_check;"

const PolarDataMaxSQL string = InternalMarkSQL + "select 1;"

const PaxosStatusSQL = InternalMarkSQL +
	"SELECT commit_index, paxos_role FROM polar_dma_member_status"

const ConnFullSQL = InternalMarkSQL +
	"SELECT COUNT(*)/ CURRENT_SETTING('max_connections')::float FROM pg_stat_activity"

const TerminateWaitQuerySQL = InternalMarkSQL +
	"select wait_event_type,wait_event,pg_terminate_backend(pid) from pg_stat_activity where application_name='ClusterManager' " +
	"and wait_event_type!='Client' and wait_event_type!='Activity' and query_start < now() - interval '10 seconds' order by query_start"

const WaitEventSQL = InternalMarkSQL +
	"select wait_event_type,wait_event from pg_stat_activity where application_name='ClusterManager' " +
	"and wait_event_type!='Client' and wait_event_type!='Activity' order by backend_start"

const ReplicaDelayTimeSQL = InternalMarkSQL +
	"select application_name, write_lag, pg_wal_lsn_diff(pg_current_wal_lsn(), write_lsn) as write_lag_size, " +
	"replay_lag,  pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) as replay_lag_size from pg_stat_replication"

const ReplicaDelayDataSizeSQL = InternalMarkSQL +
	"select slot_name, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) from pg_replication_slots"

const RoReplayBgReplayDiffSQL = InternalMarkSQL + "select pg_wal_lsn_diff(pg_last_wal_replay_lsn(), polar_replica_bg_replay_lsn()) as size"

const RecoveryReplaySizeSQL = InternalMarkSQL +
	"select pg_wal_lsn_diff(pg_current_wal_insert_lsn(), polar_consistent_lsn())"

const CopyBufferUsageSQL = InternalMarkSQL + "create extension if not exists polar_monitor; select polar_cbuf()"

const CopyBufferSettingSQL = InternalMarkSQL + "select setting from pg_settings where name='polar_copy_buffers'"

const SharedBufferUsageSQL = InternalMarkSQL + "create extension if not exists polar_monitor; select polar_flushlist()"

const SharedBufferSettingSQL = InternalMarkSQL + "select setting from pg_settings where name='shared_buffers'"

const CollectFileConfSQL = InternalMarkSQL +
	"select current_setting(t1.name) as user_setting, t1.name as name, t1.setting as file_setting, t2.context as context, t1.sourcefile, t1.sourceline, t1.error, t1.applied, t2.unit as unit, t2.vartype from pg_file_settings t1 left join pg_settings t2 on t1.name = t2.name where t1.sourcefile like '%postgresql%conf' order by name, error desc"

const CollectFileConfTimestampSQL = InternalMarkSQL + "select pg_conf_load_time();"

const CollectErrorFileConfSQL = InternalMarkSQL + "select name from pg_file_settings where error != '' limit 1"

type SwitchoverReq struct {
	From                 string `json:"from"`
	To                   string `json:"to"`
	Force                int    `json:"force"`
	Rto                  int    `json:"rto"`
	RpoAcceptableSeconds int    `json:"rpo_acceptable_seconds"`
	IgnoreRo             int    `json:"ignore_ro"`
}

type ClusterManagerVersion struct {
	Version string          `json:"version"`
	Plugins []PluginVersion `json:"plugins,omitempty"`
}

type PluginVersion struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type ClusterManagerConf struct {
	Settings map[string]interface{} `json:"settings"`
}
