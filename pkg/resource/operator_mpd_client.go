package resource

import (
	"encoding/json"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
	"time"
)

type OperatorMpdClient struct {
	Conf          MpdClientConf
	PvcName       string
	NamespaceName string
	Type          string

	Client EndpointHttpClient
}

type StorageApiInfo struct {
	SvcName      string `json:"svc_name"`
	SvcNamespace string `json:"svc_namespace"`
	PvcName      string `json:"pvc_name"`
	PvcNamespace string `json:"pvc_namespace"`
	LockSvcUrl   string `json:"lock_svc_url"`
	GetTopoUrl   string `json:"get_topo_url"`
}

type PolarStackConf struct {
	StorageApi StorageApiInfo `json:"storage_api_info"`
}

type MpdClientConf struct {
	PolarStack PolarStackConf `json:"polar_stack"`
}

func (c *OperatorMpdClient) GetType() string {
	return c.Type
}

func (c *OperatorMpdClient) UpdateMpdSvcEndpoint() []common.EndPoint {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return nil
	}
	svc, err := client.CoreV1().Services(c.Conf.PolarStack.StorageApi.SvcNamespace).Get(c.Conf.PolarStack.StorageApi.SvcName, v1.GetOptions{})
	if err != nil {
		return nil
	}

	return []common.EndPoint{
		*common.NewEndPointWithPanic(svc.Spec.ClusterIP + ":" + strconv.Itoa(int(svc.Spec.Ports[0].Port))),
	}
}

func (c *OperatorMpdClient) LoadConf(v []byte) error {
	err := json.Unmarshal(v, &c.Conf)
	if err != nil {
		return errors.Wrapf(err, "Failed to unmarshal %s", string(v))
	}

	c.PvcName = c.Conf.PolarStack.StorageApi.PvcName
	c.NamespaceName = c.Conf.PolarStack.StorageApi.PvcNamespace
	c.Client = EndpointHttpClient{
		UpdateEndpoint: c.UpdateMpdSvcEndpoint,
	}

	return nil
}

func (c *OperatorMpdClient) MasterReplicaMetaSwitch(oldRw common.InsSpec, newRw common.InsSpec, clusterID string) error {
	return nil
}

func (c *OperatorMpdClient) MasterStandbyMetaSwitch(oldRw common.InsSpec, newRw common.InsSpec) error {
	return nil
}

func (c *OperatorMpdClient) UpdateEngineStatus(ins common.InsSpec, state, reason string) error {
	return nil
}

func (c *OperatorMpdClient) ReadOnlyLock(lock bool) error {
	return nil
}

func (c *OperatorMpdClient) SwitchStore(from common.InsSpec, to common.InsSpec) (error, string) {
	url := "/pvcs/lock"
	param := map[string]interface{}{
		"name":               c.PvcName,
		"namespace":          c.NamespaceName,
		"write_lock_node_id": to.HostName,
	}

	err, body := c.Client.PostRequest(url, param, false)
	if err != nil {
		log.Warnf("Failed to switch mpd store err:%v msg:%s req:%s:%v", err, body, url, param)
		return err, ""
	}

	type PvcsLockResp struct {
		WorkflowID string `json:"workflow_id"`
	}

	var resp PvcsLockResp
	err = json.Unmarshal([]byte(body), &resp)
	if err != nil {
		log.Warnf("Failed to switch mpd store err:%v msg:%s req:%s:%v", err, body, url, param)
		return err, ""
	}

	url = "/pvcs/topo"
	param = map[string]interface{}{
		"name":       c.PvcName,
		"namespace":  c.NamespaceName,
		"workflowId": resp.WorkflowID,
	}

	type StorageTopoNode struct {
		NodeId string `json:"node_id"`
		NodeIP string `json:"node_ip"`
	}

	type StorageTopo struct {
		ReadNodes []*StorageTopoNode `json:"read_nodes"`
		WriteNode *StorageTopoNode   `json:"write_node"`
	}

	var topo StorageTopo
	for checkCnt := 0; ; {
		err, body := c.Client.GetRequest(url, param, false)
		if err == nil {
			err = json.Unmarshal([]byte(body), &topo)
			if err != nil {
				log.Warnf("Failed to unmarshal %s err %s", body, err.Error())
			} else {
				if topo.WriteNode != nil && topo.WriteNode.NodeId == to.HostName {
					return nil, ""
				}
			}
		}
		time.Sleep(time.Millisecond * 500)
		checkCnt++
		if checkCnt > 15 {
			return errors.Wrapf(err, "Failed to QueryTask %s topo %v", resp.WorkflowID, topo), ""
		}
	}
}

func (c *OperatorMpdClient) SwitchVip(from common.InsSpec, to common.InsSpec, switchType string) (error, string) {
	return nil, ""
}
