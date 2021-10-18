package action

import (
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"strconv"
	"time"
)

type ProxyReportActionExecutor struct {
	Ins     common.InsSpec
	Healthy bool
}

type ProxyHealthy struct {
	Healthy bool      `json:"healthy"`
	StartAt time.Time `json:"startAt"`
	LastAt  time.Time `json:"lastAt"`
}

func (e *ProxyReportActionExecutor) String() string {
	return "ProxyReportActionExecutor: ins: " + e.Ins.String() + " healthy: " + strconv.FormatBool(e.Healthy)
}

func (e *ProxyReportActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	pod, err := resource.GetResourceManager().GetK8sClient().GetPodInfo(e.Ins.PodName)
	if err != nil {
		log.Warnf("Failed to get pod info %s", e.Ins.String())
		return err
	}

	var proxyHealthy ProxyHealthy
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}

	masterClusterID := metaManager.GetMasterClusterID()
	rwSpec, err := metaManager.GetRwSpec(masterClusterID)

	if err != nil {
		log.Errorf(" metaManager.GetRwSpec err:%v, can't build event", err)
	} else {
		rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
		rEvent.Body.Describe = fmt.Sprintf("instance %v report maxscale unthealthy, po: %v , MaxScale insPod:%v", rwSpec.CustID, rwSpec.PodName, pod.Name)
		rEvent.EventCode = notify.EventCode_MaxScaleProxyUnhealthy
		rEvent.Level = notify.EventLevel_ERROR

		sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
		if sErr != nil {
			log.Errorf("ProxyReportActionExecutor err: %v", sErr)
		}
	}

	if healthy, exist := pod.Annotations["healthy"]; exist {
		err = json.Unmarshal([]byte(healthy), &proxyHealthy)
		if err != nil {
			log.Warnf("Unmarshal %s err %s", healthy, err.Error())
			return err
		}
		if (proxyHealthy.Healthy && e.Healthy) || (!proxyHealthy.Healthy && !e.Healthy) {
			log.Debugf("Skip to update proxy %s status to %v", e.Ins.String(), e.Healthy)
			return nil
		}
	}
	if e.Healthy {
		proxyHealthy.Healthy = true
	} else {
		proxyHealthy.Healthy = false
	}
	proxyHealthy.LastAt = proxyHealthy.StartAt
	proxyHealthy.StartAt = time.Now()
	healthyJson, err := json.Marshal(proxyHealthy)
	if err != nil {
		log.Warnf("Marshal %v err %s", proxyHealthy, err.Error())
		return err
	}
	pod.Annotations["healthy"] = string(healthyJson)
	err = resource.GetResourceManager().GetK8sClient().UpdatePodInfo(pod)
	if err != nil {
		log.Warnf("Failed to update pod %s info %s", pod.String(), e.Ins.String())
		return err
	}

	if !e.Healthy {
		log.Infof("ClusterManager remove proxy %s since unhealthy", e.Ins.String())
		statusManager.DeleteProxyStatus(e.Ins.Endpoint)
		delete(statusManager.ClusterStatus.Proxy, e.Ins.Endpoint)
	}

	log.Infof("Success to update proxy %s status to %v", e.Ins.String(), e.Healthy)
	return nil
}
