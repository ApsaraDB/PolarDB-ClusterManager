package common

import "flag"

var EngineDetectTimeoutMs = flag.Int64("engine_detect_timeout_ms", 3000, "cluster manager detect engine timeout")
var EngineDetectIntervalMs = flag.Int64("engine_detect_interval_ms", 3000, "cluster manager detect engine interval")
var EngineMetricsTimeoutMs = flag.Int64("engine_metrics_timeout_ms", 3000, "cluster manager metrics engine timeout")
var EngineMetricsIntervalMs = flag.Int64("engine_metrics_interval_ms", 3000, "cluster manager metrics engine interval")
var EngineConfIntervalMs = flag.Int64("engine_conf_interval_ms", 10000, "cluster manager collect engine conf interval")
var EnableCadvisorCollector = flag.Bool("cadvisor_collector", false, "enable cadvisor collector")
var EnableLogagentCollector = flag.Bool("logagent_collector", false, "enable logagent collector")

var EnableAllAction = flag.Bool("enable_all", true, "disable all cluster manager action")
var AutoReplicaFailover = flag.Bool("auto_replica_failover", true, "auto replica failover")
var AutoStandbyFailover = flag.Bool("auto_standby_failover", false, "auto standby failover")
var AutoStandbyReplicaFailover = flag.Bool("auto_standby_replica_failover", false, "auto standby replica failover")
var EnableRebuildRoStandby = flag.Bool("enable_rebuild_ro_standby", true, "disable auto rebuild ro standby")

var EngineRecoveryMbPerSecond = flag.Int64("engine_recovery_mb_per_second", 25, "engine recovery speed mb/s")
var EngineReplicaDelayMB = flag.Int64("engine_replica_delay_mb", 1024, "replica delay data size")
var EngineReplicaDelayMs = flag.Int64("engine_replica_delay_ms", 3600000, "replica delay time")
var EngineBufferUsageFatalRatio = flag.Int("engine_buffer_usage_fatal_ratio", 90, "buffer usage")

var StandbyRPO = flag.Int("standby_rpo", 30, "max standby faliover rpo")
var StandbyRTO = flag.Int("standby_rto", 60, "max standby switchover rto")

var EnablePolarBox = flag.Bool("enable_polar_box", false, "enable polar box")
var EnableDebugLog = flag.Bool("enable_debug_log", true, "")

var V1Compatibility = flag.Bool("v1_compatibility", true, "compatibility of v1.0")

var EnableOnlinePromote = flag.Bool("enable_online_promote", true, "enable online promote")

var WorkDir = flag.String("work_dir", "/root/polardb_cluster_manager", "ClusterManager work directory, conf/polardb_cluster_manager.conf")

var ConfigFile = flag.String("config_file", "conf/polardb_cluster_manager.conf", "ClusterManager config file")
var ConsensusPath = flag.String("consens_path", "consensus", "ClusterManager consensus data path")
var LogFile = flag.String("log_file", "polardb_cluster_manager.log", "ClusterManager log")
var WorkUser = flag.String("work_user", "root", "work user")
var EnablePolarStackInternalPolicy = flag.Bool("enable_polarstack_internal_policy", true, "enable polarstack internal optimize ha policy")

var EnableRedLine = flag.Bool("enable_redline", true, "enable redline event upload")
var EnableAutoUpdatePXConf = flag.Bool("enable_auto_update_px_conf", false, "enable auto update PX conf action")
var EnableSyncRoConf = flag.Bool("enable_sync_ro_conf", false, "enable auto sync ro postgresql.conf from rw")
var EventLogPathPrefix = flag.String("event_log_path_prefix", "/var/log/polardb-cm", "prefix of event log")
var EnableEventLog = flag.Bool("enable_event_log", true, "enable write event log to disk")

var PluginConfPath = flag.String("plugin_conf_path", "/usr/local/polardb_cluster_manager/bin/plugin", "plugin conf path")
var HaPolicyConfPath = flag.String("ha_policy_conf_path", "conf/polardb_ha_policy.conf", "ClustrManager ha policy conf")

var HaMode = flag.String("ha_mode", "auto", "ha mode [auto/manual]")

const (
	EnableAllFlagType            string = "enable_all"
	EngineDetectTimeoutFlagType  string = "engine_detect_timeout_ms"
	SQLMonitorFlagType           string = "sql_monitor"
	EngineReplicaDelayMsFlagType string = "engine_replica_delay_ms"
	EnableOnlinePromoteFlagType  string = "enable_online_promote"
	EnableRebuildRoStandbyType   string = "enable_rebuild_ro_standby"
	EnableRedLineType            string = "enable_redline"
	EnableAutoUpdatePXConfType   string = "enable_auto_update_px_conf"
)
