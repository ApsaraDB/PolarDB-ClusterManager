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
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

type UpdateRunningConfActionExecutor struct {
	Timestamp time.Time
}

func (e *UpdateRunningConfActionExecutor) String() string {
	return fmt.Sprintf("UpdateRunningConfActionExecutor: on %s", e.Timestamp)
}

func (e *UpdateRunningConfActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {

	c, _, err := resource.GetKubeClient(time.Second * 10)
	if err != nil {
		log.Warnf("Failed to update running conf err=%s", err.Error())
		return err
	}

	cmName := resource.GetClusterManagerConfig().Cluster.Name + "-running-params"
	cm, err := c.CoreV1().ConfigMaps(resource.GetClusterManagerConfig().Cluster.Namespace).Get(cmName, v1.GetOptions{})
	if err != nil {
		log.Warnf("Failed to get running conf %s err=%s", cmName, err.Error())
		return err
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}

	masterClusterID := metaManager.GetMasterClusterID()
	rwSpec, err := metaManager.GetRwSpec(masterClusterID)

	if err != nil {
		log.Errorf(" metaManager.GetRwSpec err:%v, can't build event", err)
	} else {
		rEvent := notify.BuildDBEventPrefixByInsV2(rwSpec, common.RwEngine, rwSpec.Endpoint)
		rEvent.Body.Describe = fmt.Sprintf("instance %v UpdateRunningConf, po: %v , config:%v", rwSpec.CustID, rwSpec.PodName, cmName)
		rEvent.EventCode = notify.EventCode_UpdateRunningConf
		rEvent.Level = notify.EventLevel_INFO

		sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
		if sErr != nil {
			log.Errorf("UpdateRunningConfActionExecutor err: %v", sErr)
		}
	}

	type Setting struct {
		Name             string    `json:"name"`
		Value            string    `json:"value"`
		InoperativeValue string    `json:"inoperative_value,omitempty"`
		UpdateTime       time.Time `json:"updateTime"`
	}

	cmData := map[string]int{}
	for _, v := range metaManager.ClusterFileConf.Conf {
		hasUpdate := false
		var setting Setting

		if cmV, exist := cm.Data[v.Name]; exist {
			err = json.Unmarshal([]byte(cmV), &setting)
			if err != nil {
				log.Warnf("Failed to unmarshal %s err=%s", cmV, err.Error())
				continue
			}

			if v.Vartype == "string" {
				if setting.Value != "'"+v.FileSetting+"'" {
					setting.Value = "'" + v.FileSetting + "'"
					setting.UpdateTime = e.Timestamp
					hasUpdate = true
				}

				if setting.InoperativeValue != "'"+v.UnAppliedFileSetting+"'" {
					if v.UnAppliedFileSetting == "" {
						if setting.InoperativeValue != "" {
							setting.InoperativeValue = ""
							setting.UpdateTime = e.Timestamp
							hasUpdate = true
						}
					} else {
						setting.InoperativeValue = "'" + v.UnAppliedFileSetting + "'"
						setting.UpdateTime = e.Timestamp
						hasUpdate = true
					}
				}
			} else {
				if setting.Value != v.FileSetting {
					setting.Value = v.FileSetting
					setting.UpdateTime = e.Timestamp
					hasUpdate = true
				}

				if setting.InoperativeValue != v.UnAppliedFileSetting {
					setting.InoperativeValue = v.UnAppliedFileSetting
					setting.UpdateTime = e.Timestamp
					hasUpdate = true
				}
			}

			if hasUpdate {
				dataV, _ := json.Marshal(setting)
				cm.Data[setting.Name] = string(dataV)
			}
		} else {
			setting.Name = v.Name
			if v.Vartype == "string" {
				setting.Value = "'" + v.FileSetting + "'"
				if v.UnAppliedFileSetting != "" {
					setting.InoperativeValue = "'" + v.UnAppliedFileSetting + "'"
				}
			} else {
				setting.Value = v.FileSetting
				setting.InoperativeValue = v.UnAppliedFileSetting
			}
			setting.UpdateTime = e.Timestamp
			dataV, _ := json.Marshal(setting)
			cm.Data[setting.Name] = string(dataV)
			hasUpdate = true
		}
		cmData[v.Name] = 0

		if hasUpdate {
			log.Infof("Update Running Conf %v with timestamp %s", setting, e.Timestamp.String())
		}
	}
	for k, _ := range cm.Data {
		if _, exist := cmData[k]; !exist {
			delete(cm.Data, k)
			log.Infof("Update Running Conf delete %s with timestamp %s", k, e.Timestamp.String())
		}
	}

	_, err = c.CoreV1().ConfigMaps(resource.GetClusterManagerConfig().Cluster.Namespace).Update(cm)
	if err != nil {
		log.Warnf("Failed to update running conf %s err=%s", cmName, err.Error())
		return err
	}

	log.Infof("Success to update config map %s with %v", cmName, cm.Data)

	return nil
}
