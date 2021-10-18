package action

import (
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"strings"
	"time"
)

type AddInsActionExecutor struct {
	InsSpec    common.InsSpec
	RwSpec     common.InsSpec
	EngineType common.EngineType

	metaManager   *meta.MetaManager
	statusManager *status.StatusManager
}

func (e *AddInsActionExecutor) String() string {
	return "AddInsActionExecutor: ins:" + e.InsSpec.String() + " rw: " + e.RwSpec.String() + " type: " + string(e.EngineType)
}

func (e *AddInsActionExecutor) Execute(m *meta.MetaManager, s *status.StatusManager) error {
	e.metaManager = m
	e.statusManager = s

	if err := e.InitializeInsManager(); err != nil {
		log.Errorf("InitializeInsManager: ins:%v rw: %v type: %v: InitializeInsManager err: %v", e.InsSpec.String(), e.RwSpec.String(), string(e.EngineType), err)
		return err
	}

	log.Infof("ins:%v rw: %v type: %v: InitializeInsStatus ", e.InsSpec.String(), e.RwSpec.String(), string(e.EngineType))

	rEvent := notify.BuildDBEventPrefixByInsV2(&e.InsSpec, e.EngineType, e.InsSpec.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("Add instance %v to cluster. po: %s", e.InsSpec.CustID, e.InsSpec.PodName)
	rEvent.EventCode = notify.EventCode_AddInstance
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("AddInsActionExecutor err: %v", sErr)
	}

	// 1. 初始化引擎实例的元数据
	e.InitializeInsStatus()

	// 3. 执行加入引擎实例的具体操作，包括recovery.conf、replication_slot
	if err := e.AddInsIntoCluster(); err != nil {
		// 操作执行失败，删掉对应的enginePhase，这样Decision流程中会重新创建新的AddInsAction
		delete(e.metaManager.ClusterMeta.SubCluster[e.InsSpec.ClusterID].EnginePhase, e.InsSpec.Endpoint.String())

		log.Errorf("ins:%v rw: %v type: %v: AddInsIntoCluster err:%v ", e.InsSpec.String(), e.RwSpec.String(), string(e.EngineType), err)

		return errors.Wrapf(err, "Failed to add %s %s to cluster", e.EngineType, e.InsSpec.String())
	}

	log.Infof("ins:%v rw: %v type: %v: AddInsIntoCluster done ", e.InsSpec.String(), e.RwSpec.String(), string(e.EngineType))

	// 4. 开始引擎实例状态采集
	e.StartInsStateMachine()

	log.Infof("ins:%v rw: %v type: %v: StartInsStateMachine done ", e.InsSpec.String(), e.RwSpec.String(), string(e.EngineType))
	return nil
}

func (e *AddInsActionExecutor) InitializeInsManager() error {
	// 初始化引擎镜像工具接口
	err := resource.GetResourceManager().UpdateInstanceResource(e.metaManager.ClusterSpec)
	if err != nil {
		return errors.Wrapf(err, "Failed to update instance resource manager")
	}

	if resource.GetResourceManager().GetEngineManager(e.InsSpec.Endpoint) == nil {
		return errors.Errorf("Failed to initialize ins manager %s", e.InsSpec.String())
	}

	return nil
}

func (e *AddInsActionExecutor) InitializeInsStatus() error {
	if _, ok := e.metaManager.ClusterMeta.SubCluster[e.InsSpec.ClusterID].EnginePhase[e.InsSpec.Endpoint.String()]; !ok {
		e.metaManager.CreateInsPhase(&e.InsSpec, common.EnginePhasePending)
	}

	return nil
}

func (e *AddInsActionExecutor) AddInsIntoCluster() error {
	if e.EngineType == common.RwEngine {
		e.statusManager.CreateOrUpdateCmStatus(&e.InsSpec)
	}

	if e.EngineType == common.RwEngine {
		if e.InsSpec.ClusterType == common.PaxosCluster {
		}
	} else if e.EngineType == common.RoEngine || e.EngineType == common.StandbyEngine {
		slotName := resource.GenerateSlotName(e.EngineType, &e.InsSpec)
		if e.InsSpec.ClusterType == common.PaxosCluster {

			// 1. update recovery.conf & restart
			sql := `select application_name from pg_stat_replication where application_name='` + slotName + `'`
			type StandbyName struct {
				ApplicationName string
			}
			var standby StandbyName
			rw := resource.GetResourceManager().GetSingleConn(e.RwSpec.Endpoint, time.Second*180)
			defer rw.Close()
			_, err := rw.QueryOne(&standby, sql)
			if err != nil {
				log.Infof("%s replication %s not exists err %s, try to demote.", e.InsSpec.String(), err.Error(), slotName)

				engineType := e.EngineType
				if e.InsSpec.IsDataMax {
					engineType = common.DataMax
				}

				newEngine := resource.GetResourceManager().GetEngineManager(e.InsSpec.Endpoint)
				if engineType == common.DataMax {
					err = newEngine.Demote(engineType, e.RwSpec, false, false)
				} else {
					err = newEngine.Start(1)
				}
				if err != nil {
					return errors.Wrapf(err, "Failed to start %s as %s", e.InsSpec.String(), e.EngineType)
				}

				// alter system add consensus follower "ip2:port2"
				role := "follower"
				if e.InsSpec.SwitchPriority == -1 {
					role = "learner"
				}
				sql := `alter system dma add ` + role + ` '` + e.InsSpec.Endpoint.String() + `'`
				_, err := rw.Exec(sql)
				if err != nil {
					if strings.Contains(err.Error(), "Failed to execute consensus command: Timeout") {
						log.Warnf("Failed to exec %s err %s skip it", sql, err.Error())
					} else {
						return errors.Wrapf(err, "Failed to add follower with sql %s for %s", sql, e.InsSpec.String())
					}
				}

				e.metaManager.SetInsPhase(&e.InsSpec, common.EnginePhaseStarting)
			}
		} else {
			// 1. setup replication slot
			err := ExecuteWithTimeoutRetry(e.SetupReplicationSlots, 3, time.Second, false)
			if err != nil {
				log.Warnf("Failed setup replication slots for ins %s to %s err %s", e.InsSpec.String(), e.EngineType, err.Error())
			}

			// 2. update recovery.conf & restart
			sql := `select application_name from pg_stat_replication where application_name='` + slotName + `'`
			type StandbyName struct {
				ApplicationName string
			}
			var standby StandbyName
			db := resource.GetResourceManager().GetMetricsConn(e.InsSpec.Endpoint)
			_, err = db.QueryOne(&standby, sql)
			if err != nil {
				log.Infof("%s replication %s not exists err %s, try to demote.", e.InsSpec.String(), err.Error(), slotName)
				newEngine := resource.GetResourceManager().GetEngineManager(e.InsSpec.Endpoint)
				err = newEngine.Demote(e.EngineType, e.RwSpec, false, false)
				if err != nil {
					log.Warnf("Failed to demote %s to %s err %s continue", e.InsSpec.String(), e.EngineType, err.Error())
				}
				e.metaManager.SetInsPhase(&e.InsSpec, common.EnginePhaseStarting)
			}
		}
	} else {
		panic(nil)
	}

	if e.EngineType == common.RwEngine {
		e.metaManager.ClusterMeta.MasterClusterID = e.InsSpec.ClusterID
		e.metaManager.ClusterMeta.SubCluster[e.InsSpec.ClusterID].RwEndpoint = e.InsSpec.Endpoint
	} else if e.EngineType == common.StandbyEngine {
		e.metaManager.ClusterMeta.SubCluster[e.InsSpec.ClusterID].RwEndpoint = e.InsSpec.Endpoint
	}
	e.metaManager.ClusterMeta.SubCluster[e.InsSpec.ClusterID].ClusterType = e.InsSpec.ClusterType

	// Update Ins

	spec := e.metaManager.ClusterSpec.SubCluster[e.InsSpec.ClusterID].InstanceSpec[e.InsSpec.Endpoint.String()]
	if spec.PrevSpec != nil {
		log.Infof(" update ins for pod change : %v->%v, recover status", e.InsSpec.PrevSpec.PodName, e.InsSpec.PodName)

		spec.PrevSpec = nil

		phase, err := e.metaManager.GetEnginePhase(e.InsSpec.Endpoint)
		if err != nil {
			log.Warnf("Failed to get engine %s phase err %s", e.InsSpec.String(), err.Error())
		} else {
			err := resource.GetResourceManager().GetOperatorClient().UpdateEngineStatus(e.InsSpec, phase, "")
			if err != nil {
				return errors.Wrapf(err, "Failed to update %s status cluster:%s id:%s state:%s err:%s",
					e.EngineType, e.InsSpec.ClusterID, e.InsSpec.ID, phase, err.Error())
			}
		}
	}

	ExecuteWithRetry(e.metaManager.Sync, 10, true)

	log.Infof("Success to add %s %s to cluster", e.EngineType, e.InsSpec.String())

	return nil
}

func (e *AddInsActionExecutor) StartInsStateMachine() error {
	if _, exist := e.statusManager.ClusterStatus.SubCluster[e.InsSpec.ClusterID]; !exist {
		e.statusManager.ClusterStatus.SubCluster[e.InsSpec.ClusterID] = &status.SubClusterStatus{
			PodStatus:           make(map[common.EndPoint]status.MultiState),
			EngineStatus:        make(map[common.EndPoint]status.MultiState),
			EngineStatusStartAt: make(map[common.EndPoint]time.Time),
		}
	}

	e.statusManager.ClusterStatus.SubCluster[e.InsSpec.ClusterID].EngineStatus[e.InsSpec.Endpoint] = map[common.EventCategory]status.State{
		common.EngineEventCategory: status.State{Name: status.EngineStatusInit},
	}
	e.statusManager.ClusterStatus.SubCluster[e.InsSpec.ClusterID].EngineStatusStartAt[e.InsSpec.Endpoint] = time.Now()
	e.statusManager.ClusterStatus.SubCluster[e.InsSpec.ClusterID].PodStatus[e.InsSpec.Endpoint] = map[common.EventCategory]status.State{
		common.PodEventCategory: status.State{Name: status.PodStatusUnknown},
	}

	e.statusManager.CreateOrUpdateEngineStatus(e.InsSpec.ClusterID, &e.InsSpec, e.EngineType, &e.RwSpec)
	e.statusManager.CreateOrUpdatePodStatus(e.InsSpec.ClusterID, &e.InsSpec)

	log.Infof("AddIns StartInsStateMachine create status: %v ", e.statusManager.ClusterStatus.SubCluster[e.InsSpec.ClusterID])

	return nil
}

func (e *AddInsActionExecutor) SetupReplicationSlots() error {

	// 1. clean old replication slot on ins
	insConn := resource.GetResourceManager().GetMetricsConn(e.InsSpec.Endpoint)
	type ReplicationSlot struct {
		SlotName string
	}
	var oldSlots []ReplicationSlot
	sql := common.InternalMarkSQL + "select slot_name from pg_replication_slots where slot_type='physical' " +
		"and (slot_name like 'replica_%' or slot_name like 'standby_%')"
	_, err := insConn.Query(&oldSlots, sql)
	if err != nil {
		log.Errorf("Failed to query old slot with sql %s err %s", sql, err.Error())
	}
	for _, oldSlot := range oldSlots {
		sql := common.InternalMarkSQL + `select pg_drop_replication_slot('` + oldSlot.SlotName + `')`
		_, err := insConn.Exec(sql)
		if err != nil {
			if !strings.Contains(err.Error(), "is active") {
				log.Errorf("Failed to drop slot with sql %s err %s", sql, err.Error())
			} else {
				log.Infof("slot %s is active", oldSlot.SlotName)
			}
		} else {
			log.Infof("Success to drop slot %s", oldSlot.SlotName)
		}
	}

	// 2. create new replication slots on rw
	slotName := resource.GenerateSlotName(e.EngineType, &e.InsSpec)
	rwConn := resource.GetResourceManager().GetMetricsConn(e.RwSpec.Endpoint)
	sql = `select pg_create_physical_replication_slot('` + slotName + `')`
	_, err = rwConn.Exec(sql)
	if err != nil {
		if !strings.Contains(err.Error(), common.DUPLICATE_OBJECT_ERROR) {
			return errors.Wrapf(err, "Failed to create slot with sql %s on %s", sql, e.InsSpec.String())
		} else {
			log.Infof("create slot %s to %s already exists, just continue", slotName, e.InsSpec.String())
		}
	} else {
		log.Infof("Success to create slot %s", slotName)
	}

	return nil
}
