package notify

const (
	//事件级别
	EventLevel_WARN     RedLineEventLevel = "WARN"
	EventLevel_INFO     RedLineEventLevel = "INFO"
	EventLevel_ERROR    RedLineEventLevel = "ERROR"
	EventLevel_CRITICAL RedLineEventLevel = "CRITICAL"
)

const (
	EventCode_UNKNOWN RedLineEventCode = "UNKNOWN"
	EventCode_Others  RedLineEventCode = "Others"

	//实例的元数据状态转为NotRunning
	EventCode_InsNotRunning RedLineEventCode = "InstanceNotRunning"
	//实例的元数据状态转为Running
	EventCode_InsRunning RedLineEventCode = "InstanceRunning"

	EventCode_AddInstance RedLineEventCode = "AddInstance"

	EventCode_InstanceDetectError    RedLineEventCode = "InstanceDetectError"
	EventCode_InstanceDetectTimeOut  RedLineEventCode = "InstanceDetectTimeOut"
	EventCode_InstanceConnectionFull RedLineEventCode = "InstanceConnectionFull"

	EventCode_InstanceSlotCleaned RedLineEventCode = "InstanceSlotCleaned"

	EventCode_RoInstanceSlotDrop RedLineEventCode = "RoInstanceSlotDropped"

	EventCode_RoInstanceDrop RedLineEventCode = "RoInstanceDropped"

	EventCode_RemoveClusterInstance RedLineEventCode = "RemoveClusterInstance"

	EventCode_LockingInstance RedLineEventCode = "LockingInstance"

	EventCode_LockInstanceError RedLineEventCode = "LockInstanceError"

	EventCode_LockInstanceDone RedLineEventCode = "LockInstanceDone"

	EventCode_UnLockInstanceDone RedLineEventCode = "UnlockInstanceDone"

	EventCode_UpateaInstanceConfig RedLineEventCode = "InstanceUpdateConfig"

	EventCode_PormotionTaskCreated RedLineEventCode = "InstancePromotionTaskCreated"

	EventCode_SwitchTaskCreated RedLineEventCode = "InstanceSwitchTaskCreated"

	EventCode_RestartTaskCreated RedLineEventCode = "InstanceRestartTaskCreated"

	EventCode_InstanceUnavailable RedLineEventCode = "InstanceUnavailable"

	EventCode_SwitchTaskDone_RwToRo RedLineEventCode = "SwitchingDone_RwToRo"

	EventCode_SwitchTaskDone_RoToRw RedLineEventCode = "SwitchingDone_RoToRw"

	EventCode_SwitchTaskRollBackStore_RwToRo RedLineEventCode = "RollbackStoreBegin_RwToRo"

	EventCode_SwitchTaskRollBackStore_RoToRw RedLineEventCode = "RollbackStoreBegin_RoToRw"

	EventCode_SwitchTaskStepDone RedLineEventCode = "SwitchStepDone"

	EventCode_SwitchBegin_RwToRo RedLineEventCode = "SwitchBegin_RwToRo"
	EventCode_SwitchBegin_RoToRw RedLineEventCode = "SwitchBegin_RoToRw"

	EventCode_SwitchStoreBegin_RwToRo RedLineEventCode = "SwitchStoreBegin_RwToRo"
	EventCode_SwitchStoreBegin_RoToRw RedLineEventCode = "SwitchStoreBegin_RoToRw"

	//0512新增
	EventCode_MESSAGE_INSTANCE_ALIVE  RedLineEventCode = "instance alive"
	EventCode_MESSAGE_INSTANCE_LOSS   RedLineEventCode = "instance loss"
	EventCode_MESSAGE_INSTANCE_DOWN   RedLineEventCode = "instance down"
	EventCode_MESSAGE_INSTANCE_SWITCH RedLineEventCode = "instance switching"
	EventCode_MESSAGE_VIP_ALIVE       RedLineEventCode = "vip alive"
	EventCode_MESSAGE_VIP_LOSS        RedLineEventCode = "vip loss"

	EventCode_InstanceStorageFull    RedLineEventCode = "InstanceStorageFull"
	EventCode_InstanceStorageNotFull RedLineEventCode = "InstanceStorageNotFull"

	EventCode_DbTopologyChange RedLineEventCode = "DbTopologyChange"

	EventCode_StopClusterDecision  RedLineEventCode = "StopClusterDecision"
	EventCode_StartClusterDecision RedLineEventCode = "StartClusterDecision"

	EventCode_CmAlarm RedLineEventCode = "ClusterManagerAlarm"

	EventCode_MaxScaleProxyUnhealthy RedLineEventCode = "MaxScaleProxyUnhealthy"

	EventCode_PhaseChange RedLineEventCode = "PhaseChange"

	EventCode_UpdateRunningConf RedLineEventCode = "UpdateRunningConf"

	EventCode_PromoteOnline  RedLineEventCode = "PromoteOnline"
	EventCode_PromoteOffLine RedLineEventCode = "PromoteOffLine"
)
