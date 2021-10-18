package service

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/decision"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/manager"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/plugin"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"io/ioutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"net/http"
	"time"
)

type ApiService struct {
	ServiceCtrl plugin.ServiceCtrl
}

type InsSpecReq struct {
	ID                  string            `json:"id,omitempty"`
	PodName             string            `json:"pod_name,omitempty"`
	IP                  string            `json:"ip,omitempty"`
	Port                string            `json:"port,omitempty"`
	Type                common.EngineType `json:"type,omitempty"`
	User                string            `json:"user,omitempty"`
	DataPath            string            `json:"dataPath,omitempty"`
	Sync                common.SyncType   `json:"sync,omitempty"`
	NodeDriverClusterID string            `json:"nodeDriverClusterID,omitempty"`
	NodeDriverMemberID  string            `json:"nodeDriverMemberID,omitempty"`
	StorageType         string            `json:"storage_type,omitempty"`
	UseNodeDriver       string            `json:"use_node_driver,omitempty"`
	HostName            string            `json:"host_name,omitempty"`
	CustID              string            `json:"cust_id,omitempty"`
	InsID               string            `json:"ins_id,omitempty"`
	ClusterID           string            `json:"cluster_id,omitempty"`
}

type ServiceResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg,omitempty"`
	Data string `json:"data,omitempty"`
}

func NewApiService(port int) *ApiService {

	s := &ApiService{}

	s.ServiceCtrl.Init(port)

	s.ServiceCtrl.AddRoute("/v1/switchover", s.ManualSwitchover)
	s.ServiceCtrl.AddRoute("/v1/enable", s.Enable)
	s.ServiceCtrl.AddRoute("/v1/status", s.Status)
	s.ServiceCtrl.AddRoute("/v1/version", s.Version)
	s.ServiceCtrl.AddRoute("/v1/add_ins", s.AddIns)
	s.ServiceCtrl.AddRoute("/v1/remove_ins", s.RemoveIns)
	s.ServiceCtrl.AddRoute("/v1/switch_log", s.SwitchLog)
	s.ServiceCtrl.AddRoute("/v1/update_px_conf", s.UpdatePXConf)
	s.ServiceCtrl.AddRoute("/v1/enable_auto_update_px_conf", s.EnableAutoUpdatePXConf)
	s.ServiceCtrl.AddRoute("/v1/lock", s.Lock)
	s.ServiceCtrl.AddRoute("/v1/unlock", s.Unlock)
	s.ServiceCtrl.AddRoute("/v1/restart", s.Restart)
	s.ServiceCtrl.AddRoute("/v1/stop", s.StopCluster)
	s.ServiceCtrl.AddRoute("/v1/start", s.StartCluster)
	s.ServiceCtrl.AddRoute("/v1/exec", s.Exec)
	s.ServiceCtrl.AddRoute("/v1/set_vip", s.SetVip)
	s.ServiceCtrl.AddRoute("/v1/unset_vip", s.UnSetVip)
	s.ServiceCtrl.AddRoute("/v1/add_vip", s.AddVip)
	s.ServiceCtrl.AddRoute("/v1/remove_vip", s.RemoveVip)
	s.ServiceCtrl.AddRoute("/v1/add_voter", s.AddVoter)
	s.ServiceCtrl.AddRoute("/v1/remove_voter", s.RemoveVoter)
	s.ServiceCtrl.AddRoute("/v1/add_proxy", s.AddProxy)
	s.ServiceCtrl.AddRoute("/v1/remove_proxy", s.RemoveProxy)
	s.ServiceCtrl.AddRoute("/v1/ha_mode", s.HaMode)
	s.ServiceCtrl.AddRoute("/v1/topology", s.HandleTopology)
	s.ServiceCtrl.AddRoute("/v1/get_decision_route", s.GetDecisionRoute)
	s.ServiceCtrl.AddRoute("/v1/add_decision_path", s.AddDecisionPath)
	s.ServiceCtrl.AddRoute("/v1/remove_decision_path", s.RemoveDecisionPath)
	s.ServiceCtrl.AddRoute("/v1/cm_leader_transfer", s.CmLeaderTransfer)

	return s
}

func (s *ApiService) ResponseSuccess(rsp http.ResponseWriter) {
	r := ServiceResp{
		Code: 200,
	}

	v, _ := json.Marshal(r)
	rsp.Write(v)
}

func (s *ApiService) ResponseError(rsp http.ResponseWriter, err error) {
	r := ServiceResp{
		Code: 500,
		Msg:  err.Error(),
	}

	v, _ := json.Marshal(r)
	rsp.Write(v)

	log.Info("Failed to service err=", err.Error())
}

func (s *ApiService) ResponseData(rsp http.ResponseWriter, data string) {
	r := ServiceResp{
		Code: 200,
		Data: data,
	}

	v, _ := json.Marshal(r)
	rsp.Write(v)
}

func (s *ApiService) ResponseResult(rsp http.ResponseWriter, result string) {
	templ := `{"code":200, "msg": "` + result + `"}`
	rsp.Write([]byte(templ))
	log.Infof("Success to service res=%s", result)
}

type ExecReq struct {
	Cmd     []string `json:"cmd"`
	Retry   int      `json:"retry"`
	Timeout int      `json:"timeout"`
}

type AddVipReq struct {
	Interface string `json:"interface"`
	Vip       string `json:"vip"`
	Mask      string `json:"mask"`
}

type RemoveVipReq struct {
	Vip string `json:"vip"`
}

type AddVoterReq struct {
	ConsensusEndpoint string `json:"consensus_endpoint"`
	ServiceEndpoint   string `json:"service_endpoint"`
}

func (s *ApiService) Start() {
	go func() {
		err := s.ServiceCtrl.Start()
		if err != http.ErrServerClosed {
			log.Fatalf("Failed to listenAndServe api service err %s", err.Error())
			common.StopFlag = true
		}
	}()
}

func (s *ApiService) Stop() error {
	s.ServiceCtrl.Stop()
	return nil
}

func (s *ApiService) SetVip(rsp http.ResponseWriter, req *http.Request) {
	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()

	var setVipReq AddVipReq
	err = json.Unmarshal(buffer, &setVipReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	log.Info("Serve LocalSetVip ", setVipReq, string(buffer))

	m, err := resource.NewNetworkManager(setVipReq.Vip, setVipReq.Interface)
	if err != nil {
		log.Errorf("Failed to new network manager %v err %s", setVipReq, err.Error())
		s.ResponseError(rsp, err)
		return
	}

	err = m.LocalSetVip()
	if err != nil {
		log.Errorf("Failed to set vip %v err %s", setVipReq, err.Error())
		s.ResponseError(rsp, err)
		return
	}

	rsp.Write([]byte(common.RESPONSE_OK))
}

func (s *ApiService) UnSetVip(rsp http.ResponseWriter, req *http.Request) {
	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()

	var setVipReq AddVipReq
	err = json.Unmarshal(buffer, &setVipReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	log.Info("Serve UnSetVip ", setVipReq, string(buffer))

	m, err := resource.NewNetworkManager(setVipReq.Vip, setVipReq.Interface)
	if err != nil {
		log.Errorf("Failed to new network manager %v err %s", setVipReq, err.Error())
		s.ResponseError(rsp, err)
		return
	}

	err = m.LocalUnsetVip()
	if err != nil {
		log.Errorf("Failed to unset vip %v err %s", setVipReq, err.Error())
		s.ResponseError(rsp, err)
		return
	}

	rsp.Write([]byte(common.RESPONSE_OK))
}

func (s *ApiService) Exec(rsp http.ResponseWriter, req *http.Request) {
	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()

	var execReq ExecReq
	err = json.Unmarshal(buffer, &execReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	log.Info("Serve Exec ", execReq, string(buffer))

	executor := resource.HostCommandExecutor{}
	out, _, err := executor.LocalExec(execReq.Cmd, execReq.Retry, time.Duration(execReq.Timeout)*time.Second)
	if err != nil {
		log.Errorf("Failed to execute %v err %s", execReq, err.Error())
		s.ResponseError(rsp, err)
		return
	}

	s.ResponseResult(rsp, out)
}

func (s *ApiService) Redirect(rsp http.ResponseWriter, req *http.Request) {
	if s.ServiceCtrl.Leader == "" {
		s.ResponseError(rsp, errors.Errorf("Failed to request cause no CM leader!"))
		return
	}
	url := "http://" + s.ServiceCtrl.Leader + req.RequestURI
	log.Infof("redirect url: %v requestContent %v", url, req.Body)
	httpReq, err := http.NewRequest(req.Method, url, req.Body)
	httpReq.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{}
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		log.Warnf("Failed to redirect to %s err %s", s.ServiceCtrl.Leader, err.Error())
		s.ResponseError(rsp, errors.Wrapf(err, "Failed to redirect to %s", s.ServiceCtrl.Leader))
		return
	}
	defer httpResp.Body.Close()
	data, _ := ioutil.ReadAll(httpResp.Body)
	rsp.Write(data)
}

func (s *ApiService) StopCluster(rsp http.ResponseWriter, req *http.Request) {
	log.Info("Serve Stop")
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	err := manager.GetClusterManager().StopCluster()
	if err != nil {
		s.ResponseError(rsp, err)
		return
	}

	s.ResponseSuccess(rsp)
}

func (s *ApiService) StartCluster(rsp http.ResponseWriter, req *http.Request) {
	log.Info("Serve Start")
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	err := manager.GetClusterManager().StartCluster()
	if err != nil {
		s.ResponseError(rsp, err)
		return
	}

	s.ResponseSuccess(rsp)
}

func (s *ApiService) Version(rsp http.ResponseWriter, req *http.Request) {
	log.Info("Serve Version")
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}
	rsp.Write([]byte(manager.GetClusterManager().Version()))
}

func (s *ApiService) Status(rsp http.ResponseWriter, req *http.Request) {
	log.Info("Serve Status")
	if manager.GetClusterManager().Role == common.FollowerRole {
		if v, exist := req.Form["type"]; exist && len(v) == 1 && v[0] == "cm" {
		} else {
			s.Redirect(rsp, req)
			return
		}
	}
	req.ParseForm()
	if v, exist := req.Form["type"]; exist && len(v) == 1 && v[0] == "visual" {
		rsp.Write([]byte(manager.GetClusterManager().VisualStatus()))
	} else if v, exist := req.Form["type"]; exist && len(v) == 1 && v[0] == "cm" {
		rsp.Write([]byte(meta.GetConsensusService().Status()))
	} else if v, exist := req.Form["type"]; exist && len(v) == 1 && v[0] == "mode" {
		s.ResponseData(rsp, *common.HaMode)
	} else {
		rsp.Write([]byte(manager.GetClusterManager().Status()))
	}
}

func (s *ApiService) ManualSwitchover(rsp http.ResponseWriter, req *http.Request) {
	var err error
	var buffer []byte

	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	buffer, err = ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()
	log.Info("Serve Manual Switchover", string(buffer))

	var switchoverReq common.SwitchoverReq
	err = json.Unmarshal(buffer, &switchoverReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	from, err := common.NewEndPoint(switchoverReq.From)
	if err != nil || from == nil {
		log.Error("Invalid from %s", switchoverReq.From)
		s.ResponseError(rsp, err)
		return
	}
	to, err := common.NewEndPoint(switchoverReq.To)
	if err != nil || to == nil {
		log.Error("Invalid to %s", switchoverReq.From)
		s.ResponseError(rsp, err)
		return
	}
	err = manager.GetClusterManager().ManualSwitch(*from, *to, switchoverReq)
	if err != nil {
		s.ResponseError(rsp, err)
		return
	}

	s.ResponseSuccess(rsp)
}

func (s *ApiService) Enable(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()
	log.Info("Serve enable", string(buffer))

	var switchableReq SwitchableReq
	err = json.Unmarshal(buffer, &switchableReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	if switchableReq.Enable == 0 {
		err = manager.GetClusterManager().Enable(false)
	} else if switchableReq.Enable == 1 {
		err = manager.GetClusterManager().Enable(true)
	} else {
		err = errors.New("Invalid enable")
	}

	if err != nil {
		s.ResponseError(rsp, err)
	} else {
		s.ResponseSuccess(rsp)
	}

}

type SwitchableReq struct {
	Enable int `json:"enable"`
}

type HaModeReq struct {
	Mode string `json:"mode"`
}

func (s *ApiService) AddVip(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()

	var addVipReq AddVipReq
	err = json.Unmarshal(buffer, &addVipReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	log.Info("Serve AddVip ", addVipReq, string(buffer))

	err = manager.GetClusterManager().AddVip(addVipReq.Vip, addVipReq.Interface, addVipReq.Mask)
	if err != nil {
		log.Warn("Failed to add vip err=", err.Error())
		s.ResponseError(rsp, err)
		return
	}

	s.ResponseSuccess(rsp)
}

func (s *ApiService) RemoveVip(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()

	var removeVipReq RemoveVipReq
	err = json.Unmarshal(buffer, &removeVipReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	log.Info("Serve RemoveVip ", removeVipReq, string(buffer))

	err = manager.GetClusterManager().RemoveVip(removeVipReq.Vip)
	if err != nil {
		log.Warn("Failed to add vip err=", err.Error())
		s.ResponseError(rsp, err)
		return
	}

	s.ResponseSuccess(rsp)
}

func (s *ApiService) NormalizeRequest(req *InsSpecReq, isAdd bool) (*common.InsSpec, error) {
	spec := &common.InsSpec{
		PodName:    req.PodName,
		Port:       req.Port,
		EngineUser: "postgres",
	}
	if req.Sync == "" {
		spec.Sync = common.ASYNC
	} else {
		spec.Sync = req.Sync
	}
	spec.HostName = req.HostName

	if req.Type == common.RW {
		req.Type = common.RwEngine
		spec.ClusterType = common.SharedStorageCluster
	} else if req.Type == common.RO {
		req.Type = common.RoEngine
		spec.ClusterType = common.SharedStorageCluster
	} else if req.Type == common.Master {
		req.Type = common.RwEngine
		spec.ClusterType = common.SharedNothingCluster
	} else if req.Type == common.Standby {
		req.Type = common.StandbyEngine
		if req.StorageType == common.PolarStore {
			spec.ClusterType = common.SharedStorageCluster
		} else {
			spec.ClusterType = common.SharedNothingCluster
		}
	} else if req.Type == common.Readonly {
		req.Type = common.StandbyEngine
		spec.SwitchPriority = -1
		spec.ClusterType = common.SharedNothingCluster
	} else if req.Type == common.Leader {
		req.Type = common.RwEngine
		spec.ClusterType = common.PaxosCluster
		if req.UseNodeDriver != "false" {
			spec.UseNodeDriver = true
		} else {
			spec.UseNodeDriver = false
		}
	} else if req.Type == common.Follower {
		req.Type = common.StandbyEngine
		spec.ClusterType = common.PaxosCluster
		if req.UseNodeDriver != "false" {
			spec.UseNodeDriver = true
		} else {
			spec.UseNodeDriver = false
		}
	} else if req.Type == common.DataMax {
		req.Type = common.StandbyEngine
		spec.IsDataMax = true
		spec.ClusterType = common.PaxosCluster
		if req.UseNodeDriver != "false" {
			spec.UseNodeDriver = true
		} else {
			spec.UseNodeDriver = false
		}
	} else if req.Type == common.Learner {
		req.Type = common.StandbyEngine
		spec.SwitchPriority = -1
		spec.ClusterType = common.PaxosCluster
		if req.UseNodeDriver != "false" {
			spec.UseNodeDriver = true
		} else {
			spec.UseNodeDriver = false
		}
	} else {
		log.Warnf("invalid engine type %s", req.Type)
		return nil, errors.Errorf("invalid engine type %s", req.Type)
	}

	if resource.IsPolarPureMode() {
		// docker deploy
		if req.PodName != "" {
			if req.User != "" || req.DataPath != "" || req.NodeDriverMemberID != "" || req.NodeDriverClusterID != "" {
				return nil, errors.Errorf("invalid req %v pod name is not null", req)
			}
		} else {
			// rpm deploy
			if req.NodeDriverClusterID != "" && req.NodeDriverMemberID != "" {
				if req.DataPath != "" {
					return nil, errors.Errorf("conflict parameter nodeDriverClusterID and dataPath!")
				}
				spec.NodeDriverClusterID = req.NodeDriverClusterID
				spec.NodeDriverMemberID = req.NodeDriverMemberID
			} else if req.User != "" && req.DataPath != "" {
				if req.NodeDriverClusterID != "" {
					return nil, errors.Errorf("conflict parameter nodeDriverClusterID and dataPath!")
				}
				spec.EngineUser = req.User
				spec.DataPath = req.DataPath
			} else {
				return nil, errors.Errorf("invalid req %v", req)
			}
		}

		if req.CustID != "" {
			spec.CustID = req.CustID
			spec.ID = req.InsID
			spec.ClusterID = req.ClusterID
		} else {
			spec.CustID = "0"
			spec.ID = fmt.Sprintf("%x", md5.Sum([]byte(req.IP+":"+req.Port)))
		}

		log.Infof("normalize req %v", *spec)
	}

	if isAdd {
		if resource.IsPolarSharedMode() {
			_, err := resource.GetResourceManager().GetK8sClient().GetInsInfoFromK8s(spec.PodName, spec)
			if err != nil {
				return nil, err
			}

		} else if resource.IsPolarPureMode() || resource.IsPolarPaasMode() {
			ep, err := common.NewEndPoint(req.IP + ":" + req.Port)
			if err != nil {
				return nil, err
			}
			spec.Endpoint = *ep
		}
	} else {
		ep, err := common.NewEndPoint(req.IP + ":" + req.Port)
		if err != nil {
			return nil, err
		}
		spec.Endpoint = *ep
	}

	return spec, nil
}

func (s *ApiService) UpdatePXConf(rsp http.ResponseWriter, req *http.Request) {
	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()
	log.Info("UpdatePXConf", string(buffer))

	err = manager.GetClusterManager().UpdatePXConf()

	if err != nil {
		s.ResponseError(rsp, err)
	} else {
		rsp.Write([]byte(common.RESPONSE_OK))
	}
}

type EnableReq struct {
	Enable int `json:"enable"`
}

func (s *ApiService) EnableAutoUpdatePXConf(rsp http.ResponseWriter, req *http.Request) {
	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()
	log.Info("AutoUpdatePXConf enable", string(buffer))

	var enableReq EnableReq
	err = json.Unmarshal(buffer, &enableReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	if enableReq.Enable == 0 {
		err = manager.GetClusterManager().EnableAutoUpdatePXConf(false)
	} else if enableReq.Enable == 1 {
		err = manager.GetClusterManager().EnableAutoUpdatePXConf(true)
	} else {
		err = errors.New("Invalid enable")
	}

	if err != nil {
		s.ResponseError(rsp, err)
	} else {
		rsp.Write([]byte(common.RESPONSE_OK))
	}
}

func (s *ApiService) AddProxy(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()
	log.Info("Serve add proxy", string(buffer))

	var insSpecReq InsSpecReq
	err = json.Unmarshal(buffer, &insSpecReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	err = manager.GetClusterManager().AddProxy(&common.InsSpec{
		Endpoint: common.EndPoint{
			Host: insSpecReq.IP,
			Port: insSpecReq.Port,
		},
	})
	if err != nil {
		s.ResponseError(rsp, err)
		return
	}

	s.ResponseSuccess(rsp)
}

func (s *ApiService) RemoveProxy(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()
	log.Info("Serve remove proxy", string(buffer))

	var insSpecReq InsSpecReq
	err = json.Unmarshal(buffer, &insSpecReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	err = manager.GetClusterManager().RemoveProxy(&common.InsSpec{
		Endpoint: common.EndPoint{
			Host: insSpecReq.IP,
			Port: insSpecReq.Port,
		},
	})
	if err != nil {
		s.ResponseError(rsp, err)
		return
	}

	s.ResponseSuccess(rsp)
}

func (s *ApiService) AddIns(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()
	log.Info("Serve add ins", string(buffer))

	var insSpecReq InsSpecReq
	err = json.Unmarshal(buffer, &insSpecReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	spec, err := s.NormalizeRequest(&insSpecReq, true)
	if err != nil {
		log.Warnf("Failed to normalize request %v err %s", insSpecReq, err.Error())
		s.ResponseError(rsp, err)
		return
	}

	err = manager.GetClusterManager().AddIns(spec, insSpecReq.Type)
	if err != nil {
		s.ResponseError(rsp, err)
		return
	}

	err = manager.GetClusterManager().WaitInsReady(spec, time.Duration(60)*time.Second)
	if err != nil {
		s.ResponseError(rsp, err)
		return
	}

	s.ResponseSuccess(rsp)
}

func (s *ApiService) RemoveIns(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()
	log.Info("Serve remove ins", string(buffer))

	var insSpecReq InsSpecReq
	err = json.Unmarshal(buffer, &insSpecReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	spec, err := s.NormalizeRequest(&insSpecReq, false)
	if err != nil {
		log.Warnf("Failed to normalize request %v err %s", insSpecReq, err.Error())
		s.ResponseError(rsp, err)
		return
	}

	err = manager.GetClusterManager().RemoveIns(spec)
	if err != nil {
		s.ResponseError(rsp, err)
		return
	}

	err = manager.GetClusterManager().WaitInsRemove(spec, time.Duration(20)*time.Second)
	if err != nil {
		s.ResponseError(rsp, err)
		return
	}

	s.ResponseSuccess(rsp)
}

func (s *ApiService) SwitchLog(rsp http.ResponseWriter, req *http.Request) {
	log.Info("Serve switch log")

	client, _, err := resource.GetDefaultKubeClient()
	if err != nil {
		s.ResponseError(rsp, err)
		return
	}
	logs, err := client.CoreV1().ConfigMaps(resource.GetClusterManagerConfig().Cluster.Namespace).List(v1.ListOptions{
		LabelSelector: "Type=switch-log,ClusterName=" + resource.GetClusterManagerConfig().Cluster.Name,
	})
	if err != nil {
		s.ResponseError(rsp, err)
		return
	}

	type SwitchLogResp struct {
		LogItems []common.SwitchItem `json:"LogItems"`
	}
	var switchLogResp SwitchLogResp

	for _, configMap := range logs.Items {
		if v, exist := configMap.Data["log"]; exist {
			var e common.SwitchEvent
			err = json.Unmarshal([]byte(v), &e)
			if err != nil {
				s.ResponseError(rsp, err)
				return
			}
			switchLogResp.LogItems = append(switchLogResp.LogItems, e.ToSwitchItem())
		}
	}

	v, err := json.Marshal(switchLogResp)
	if err != nil {
		s.ResponseError(rsp, err)
		return
	}

	s.ResponseData(rsp, string(v))
}

func (s *ApiService) Lock(rsp http.ResponseWriter, req *http.Request) {
	log.Info("Serve lock cluster")
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	err := manager.GetClusterManager().LockReadOnly()

	if err != nil {
		s.ResponseError(rsp, err)
	} else {
		s.ResponseSuccess(rsp)
	}
}

func (s *ApiService) Unlock(rsp http.ResponseWriter, req *http.Request) {
	log.Info("Serve unlock cluster")
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	err := manager.GetClusterManager().UnlockReadOnly()

	if err != nil {
		s.ResponseError(rsp, err)
	} else {
		s.ResponseSuccess(rsp)
	}
}

func (s *ApiService) Restart(rsp http.ResponseWriter, req *http.Request) {
	log.Info("Serve restart cluster")
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	var err error = nil

	if err != nil {
		s.ResponseError(rsp, err)
	} else {
		s.ResponseSuccess(rsp)
	}
}

func (s *ApiService) CmLeaderTransfer(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()

	if len(buffer) == 0 {
		log.Info("Serve CmLeaderTransfer")

		err = meta.GetConsensusService().LeaderTransfer("", "")
		if err != nil {
			log.Warn("Failed to transfer leader err=", err.Error())
			s.ResponseError(rsp, err)
			return
		}
	} else {
		var newLeaderReq AddVoterReq
		err = json.Unmarshal(buffer, &newLeaderReq)
		if err != nil {
			log.Error("Failed to Unmarshal", string(buffer), err)
			s.ResponseError(rsp, err)
			return
		}
		log.Info("Serve CmLeaderTransfer ", newLeaderReq, string(buffer))

		err = meta.GetConsensusService().LeaderTransfer(newLeaderReq.ServiceEndpoint, newLeaderReq.ConsensusEndpoint)
		if err != nil {
			log.Warn("Failed to transfer leader err=", err.Error())
			s.ResponseError(rsp, err)
			return
		}
	}

	s.ResponseSuccess(rsp)
}

func (s *ApiService) AddVoter(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()

	var addVoterReq AddVoterReq
	err = json.Unmarshal(buffer, &addVoterReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	log.Info("Serve AddVoter ", addVoterReq, string(buffer))

	err = meta.GetConsensusService().Join(addVoterReq.ServiceEndpoint, addVoterReq.ConsensusEndpoint)
	if err != nil {
		log.Warn("Failed to add voter err=", err.Error())
		s.ResponseError(rsp, err)
		return
	}

	s.ResponseSuccess(rsp)
}

func (s *ApiService) RemoveVoter(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	buffer, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", req.Body, err)
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()

	var removeVoterReq AddVoterReq
	err = json.Unmarshal(buffer, &removeVoterReq)
	if err != nil {
		log.Error("Failed to Unmarshal", string(buffer), err)
		s.ResponseError(rsp, err)
		return
	}

	log.Info("Serve RemoveVoter ", removeVoterReq, string(buffer))

	err = meta.GetConsensusService().Remove(removeVoterReq.ServiceEndpoint, removeVoterReq.ConsensusEndpoint)
	if err != nil {
		log.Warn("Failed to remove voter err=", err.Error())
		s.ResponseError(rsp, err)
		return
	}

	s.ResponseSuccess(rsp)
}

func (s *ApiService) HaMode(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	if req.Method == "POST" {
		buffer, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Error("Failed to Read Body", req.Body, err)
			s.ResponseError(rsp, err)
			return
		}
		defer req.Body.Close()
		log.Info("Serve enable", string(buffer))

		var haModeReq HaModeReq
		err = json.Unmarshal(buffer, &haModeReq)
		if err != nil {
			log.Error("Failed to Unmarshal", string(buffer), err)
			s.ResponseError(rsp, err)
			return
		}

		err = manager.GetClusterManager().HaMode(haModeReq.Mode)

		if err != nil {
			s.ResponseError(rsp, err)
		} else {
			s.ResponseSuccess(rsp)
		}
	} else if req.Method == "GET" {
		s.ResponseData(rsp, *common.HaMode)
	}
}

func (s *ApiService) HandleTopology(rsp http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {
		topo := manager.GetClusterManager().MetaManager.GetTopologyFromMetaManager()
		rsp.Write([]byte(topo.String()))
	} else if req.Method == "POST" {
		resource.GetResourceManager().GetSmartClientService().SendByAPI()
		s.ResponseSuccess(rsp)
	} else {
		s.ResponseError(rsp, errors.Errorf("invalid method %s", req.Method))
	}
}

func (s *ApiService) GetDecisionRoute(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	if req.Method == "GET" {
		defer log.Info("Serve get_decision_route")

		res, err := decision.GetDecisionRoute().GetDecisionPath()
		if err != nil {
			s.ResponseError(rsp, err)
		} else {
			s.ResponseData(rsp, res)
		}
	} else {
		s.ResponseError(rsp, errors.Errorf("unsupport method %s", req.Method))
	}
}

func (s *ApiService) AddDecisionPath(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	if req.Method == "POST" {
		buffer, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Error("Failed to Read Body", req.Body, err)
			s.ResponseError(rsp, err)
			return
		}
		defer req.Body.Close()
		defer log.Info("Serve add_decision_path", string(buffer))

		var pathReq decision.DecisionPath
		err = json.Unmarshal(buffer, &pathReq)
		if err != nil {
			log.Error("Failed to Unmarshal", string(buffer), err)
			s.ResponseError(rsp, err)
			return
		}

		err = decision.GetDecisionRoute().AddDecisionPath(pathReq)
		if err != nil {
			s.ResponseError(rsp, err)
		} else {
			s.ResponseSuccess(rsp)
		}
	} else {
		s.ResponseError(rsp, errors.Errorf("unsupport method %s", req.Method))
	}
}

func (s *ApiService) RemoveDecisionPath(rsp http.ResponseWriter, req *http.Request) {
	if manager.GetClusterManager().Role == common.FollowerRole {
		s.Redirect(rsp, req)
		return
	}

	if req.Method == "POST" {
		buffer, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Error("Failed to Read Body", req.Body, err)
			s.ResponseError(rsp, err)
			return
		}
		defer req.Body.Close()
		defer log.Info("Serve remove_decision_path", string(buffer))

		var pathReq decision.DecisionPath
		err = json.Unmarshal(buffer, &pathReq)
		if err != nil {
			log.Error("Failed to Unmarshal", string(buffer), err)
			s.ResponseError(rsp, err)
			return
		}

		err = decision.GetDecisionRoute().RemoveDecisionPath(pathReq.ID)
		if err != nil {
			s.ResponseError(rsp, err)
		} else {
			s.ResponseSuccess(rsp)
		}
	} else {
		s.ResponseError(rsp, errors.Errorf("unsupport method %s", req.Method))
	}
}
