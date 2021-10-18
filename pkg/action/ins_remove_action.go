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

type RemoveInsActionExecutor struct {
	InsSpec    common.InsSpec
	EngineType common.EngineType
	RwSpec     common.InsSpec

	IsRemoveStatusInfo bool

	metaManager   *meta.MetaManager
	statusManager *status.StatusManager
}

func (e *RemoveInsActionExecutor) String() string {
	return "RemoveInsActionExecutor: ins:" + e.InsSpec.String() + " rw: " + e.RwSpec.String() + " type: " + string(e.EngineType)
}

func (e *RemoveInsActionExecutor) Execute(m *meta.MetaManager, s *status.StatusManager) error {
	e.metaManager = m
	e.statusManager = s

	rEvent := notify.BuildDBEventPrefixByInsV2(&e.InsSpec, common.RwEngine, e.InsSpec.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("Remove instance %v from cluster. po: %s", e.InsSpec.CustID, e.InsSpec.PodName)
	rEvent.EventCode = notify.EventCode_RemoveClusterInstance
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("RemoveInsActionExecutor err: %v", sErr)
	}

	// 1. AddIns失败的节点删除
	// AddInsAction第一步就是构造enginePhase，
	// 如果不存在的就表示没有成功执行的AddInsAction，也就不需要RemoveInsAction
	// 直接删除这个Spec
	if _, _, err := m.GetInsPhase(&e.InsSpec); err == nil {
		// 2. AddIns成功的节点删除
		// 2.1 停止状态检测
		e.StopInsStateMachine()

		// 2.2 从集群中删除,尝试3次
		maxTimes := 3
		for times := 0; times < maxTimes; times++ {
			if err := e.RemoveInsFromCluster(); err != nil {
				log.Errorf("[times:=%v]Failed to remove %s to cluster err %s", times, e.InsSpec.String(), err.Error())
				if times < maxTimes-1 {
					//非最后一次，延迟1秒处理
					time.Sleep(1 * time.Second)
				}
				continue
			}
			break
		}

		// 2.3 清理元数据
		cErr := e.CleanInsStatus()
		if cErr != nil {
			log.Errorf("Failed to CleanInsStatus %s  err %s", e.InsSpec.String(), cErr.Error())
		}
	}

	log.Infof("RemoveInsActionExecutor,RemoveInsFromCluster done %s  ", e.InsSpec.String())
	// 2.4 真正删除spec
	delete(m.ClusterSpec.SubCluster[e.InsSpec.ClusterID].InstanceSpec, e.InsSpec.Endpoint.String())
	if len(m.ClusterSpec.SubCluster[e.InsSpec.ClusterID].InstanceSpec) == 0 {
		delete(m.ClusterSpec.SubCluster, e.InsSpec.ClusterID)
	}
	//清空已有的前置关系
	e.InsSpec.PrevSpec = nil

	log.Infof("RemoveInsActionExecutor,delete from spec done %s  ", e.InsSpec.String())

	eErr := ExecuteWithRetry(e.metaManager.Sync, 10, true)
	if eErr != nil {
		log.Errorf("Failed to metaManager.Syn %s  err %s", e.InsSpec.String(), eErr.Error())
	}

	log.Infof("RemoveInsActionExecutor,meta sync done %s  ", e.InsSpec.String())

	return nil
}

func (e *RemoveInsActionExecutor) CleanInsStatus() error {
	log.Infof("RemoveInsActionExecutor CleanInsStatus for: %v[ep=%v] ,pod: %v, IsRemoveStatusInfo=%v", e.InsSpec.ID, e.InsSpec.Endpoint.String(), e.InsSpec.PodName, e.IsRemoveStatusInfo)
	delete(e.metaManager.ClusterSpec.SubCluster[e.InsSpec.ClusterID].InstanceSpec, e.InsSpec.Endpoint.String())
	delete(e.metaManager.ClusterMeta.SubCluster[e.InsSpec.ClusterID].EngineTag, e.InsSpec.Endpoint.String())
	delete(e.metaManager.ClusterMeta.SubCluster[e.InsSpec.ClusterID].EnginePhase, e.InsSpec.Endpoint.String())
	delete(e.metaManager.ClusterMeta.SubCluster[e.InsSpec.ClusterID].EnginePhaseStartAt, e.InsSpec.Endpoint.String())
	if len(e.metaManager.ClusterMeta.SubCluster[e.InsSpec.ClusterID].EnginePhase) == 0 {
		delete(e.metaManager.ClusterMeta.SubCluster, e.InsSpec.ClusterID)
	}

	delete(e.statusManager.ClusterStatus.SubCluster[e.InsSpec.ClusterID].EngineStatus, e.InsSpec.Endpoint)
	delete(e.statusManager.ClusterStatus.SubCluster[e.InsSpec.ClusterID].PodStatus, e.InsSpec.Endpoint)
	delete(e.statusManager.ClusterStatus.SubCluster[e.InsSpec.ClusterID].EngineStatusStartAt, e.InsSpec.Endpoint)

	if len(e.statusManager.ClusterStatus.SubCluster[e.InsSpec.ClusterID].EngineStatus) == 0 {
		delete(e.statusManager.ClusterStatus.SubCluster, e.InsSpec.ClusterID)
	}

	return nil
}

func (e *RemoveInsActionExecutor) RemoveInsFromCluster() error {
	if e.InsSpec.ClusterType == common.PaxosCluster {
		role := "follower"
		if e.InsSpec.SwitchPriority == -1 {
			role = "learner"
		}
		db := resource.GetResourceManager().GetMetricsConn(e.RwSpec.Endpoint)
		sql := `alter system dma drop ` + role + ` '` + e.InsSpec.Endpoint.String() + `'`
		_, err := db.Exec(sql)
		if err != nil {
			if strings.Contains(err.Error(), "Target node not exists") {
				log.Warnf("Failed to exec %s err %s skip it", sql, err.Error())
			} else {
				return errors.Wrapf(err, "Failed to drop follower with sql %s for ins %s", sql, e.InsSpec.String())
			}
		}

		// 2. stop ins, maybe failed because host down/pod not exist and so on
		newEngine := resource.GetResourceManager().GetEngineManager(e.InsSpec.Endpoint)
		err = newEngine.Stop(resource.ImmediateStop, 2)
		if err != nil {
			log.Warnf("Failed to stop instance but we can continue, err %s", err.Error())
		}
	} else {
		// 1. stop ins, maybe failed because host down/pod not exist and so on
		newEngine := resource.GetResourceManager().GetEngineManager(e.InsSpec.Endpoint)

		if e.IsRemoveStatusInfo {
			log.Infof(" RemoveInsFromCluster: %v, for pod: %v [ep=%v]  stop the engine.", e.InsSpec.ID, e.InsSpec.PodName, e.InsSpec.Endpoint.String())
			err := newEngine.Stop(resource.ImmediateStop, 2)
			if err != nil {
				log.Warnf("Failed to stop instance but we can continue, err %s", err.Error())
			}
		} else {
			log.Infof(" RemoveInsFromCluster: %v, for pod: %v [ep=%v] because of prev status skip it ......", e.InsSpec.ID, e.InsSpec.PodName, e.InsSpec.Endpoint.String())
		}

		// 2. drop slot
		slotName := resource.GenerateSlotName(e.EngineType, &e.InsSpec)
		db := resource.GetResourceManager().GetMetricsConn(e.RwSpec.Endpoint)
		sql := `select pg_drop_replication_slot('` + slotName + `')`
		_, err := db.Exec(sql)
		if err != nil {
			if !strings.Contains(err.Error(), "does not exist") {
				return errors.Wrapf(err, "Failed to drop slot with sql %s for ins %s", sql, e.InsSpec.String())
			} else {
				log.Warnf("drop slot %s for %s does not exists, just continue", slotName, e.InsSpec.String())
			}
		}

		// 3. drop synchronous_standby_names
		rw := resource.GetResourceManager().GetEngineManager(e.RwSpec.Endpoint)
		err = rw.SetSyncMode(&e.InsSpec, common.ASYNC)
		if err != nil {
			return errors.Wrapf(err, "Failed to drop synchronous_standby_names for ins %s", e.InsSpec.String())
		}
	}

	log.Infof("Success to remove %s from cluster", e.InsSpec.String())
	return nil
}

func (e *RemoveInsActionExecutor) StopInsStateMachine() error {

	e.statusManager.DeleteEngineStatus(e.InsSpec.Endpoint)
	e.statusManager.DeletePodStatus(e.InsSpec.Endpoint)

	return nil
}
