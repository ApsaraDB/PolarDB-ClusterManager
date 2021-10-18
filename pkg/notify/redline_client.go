package notify

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"io/ioutil"
	"k8s.io/apimachinery/pkg/util/wait"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const IsGzipBody = false

type RedLineClient struct {
	EndPoints string
}

var client = RedLineClient{
	EndPoints: "",
}

type ApiVersionBody struct {
	Success  bool     `json:"success,omitempty"`
	Message  string   `json:"msg,omitempty"`
	DataList []string `json:"dataList,omitempty"`
}

var initLock = sync.Mutex{}

var supportEventApi = false

var GetRedLineEpStr func() (string, error) = func() (string, error) {
	return "", fmt.Errorf("not real function")
}

var GetRedLineBackupEpStr func() (string, error) = func() (string, error) {
	return "", fmt.Errorf("not real function")
}

func NewRedLineClient() *RedLineClient {
	c := &client
	go wait.Forever(c.VersionCheck, 5*time.Minute)
	//wait.Until(c.VersionCheck, 5* time.Minute, wait.NeverStop)
	return c
}

func (red *RedLineClient) Init() {
	initLock.Lock()
	defer initLock.Unlock()

	if red.EndPoints != "" {
		log.Infof("redLine ep is :%v", red.EndPoints)
		return
	}

	log.Infof("redLine ep is null ,try get from svc")

	ep, err := GetRedLineEpStr()
	if err != nil {
		log.Errorf(" Get redLine ep err: %v, try to use backup ep next code", err)
	}
	///
	if ep != "" {
		log.Infof("set redLine ep = %v", ep)
		red.EndPoints = ep
		return
	}

	log.Errorf(" Get redLine ep err: ep is null, try to use backup ep")
	epBackup, errBackup := GetRedLineBackupEpStr()
	if errBackup != nil {
		log.Errorf(" Get redLine backup ep err: %v, ", err)
		return
	}
	if epBackup != "" {
		log.Infof("set redLine backup ep = %v", epBackup)
		red.EndPoints = epBackup
	} else {
		log.Errorf(" Get redLine epBackup err: ep is null")
		return
	}
}

var redLineTransPort = &http.Transport{
	TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true,
	},
	DialContext: (&net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 10 * time.Second,
		DualStack: true,
	}).DialContext,
	ForceAttemptHTTP2:     false,
	MaxIdleConns:          10,
	IdleConnTimeout:       15 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
	MaxConnsPerHost:       10,
}

func (red *RedLineClient) post(params map[string]interface{}) (error, string) {
	if red.EndPoints == "" {
		red.Init()
		supportEventApi = red.IsSupportV2Api()
	}
	if supportEventApi {
		log.Warnf("event api is supported, suggest to use postV2 ,and this msg be dropped:%v ", params)
		return nil, ""
	}

	jsonData, err := json.Marshal(params)
	if err != nil {
		return err, ""
	}
	requestContent := string(jsonData)

	identityKey := ""
	ep, epOk := params["endPoint"]
	if epOk {
		insEp, chOk := ep.(common.EndPoint)
		if chOk {
			//identityKey = fmt.Sprintf("%v;%v;%v", params["logicInsName"], insEp.Host, insEp.Port)
			identityKey = fmt.Sprintf("all-cm;%v;%v", insEp.Host, insEp.Port)

		}
	}

	//接口文档： https://yuque.antfin-inc.com/docs/share/76c752a0-2c82-4cdd-bafd-568d35ae0ceb?#

	postUrl := fmt.Sprintf("http://%v/data/upload", red.EndPoints)
	requestBody := jsonData
	if IsGzipBody {
		requestBody = gzipContents(requestContent)
	}

	if requestBody == nil {
		log.Errorf("null request body after gzip. msg:%v", requestContent)
		return fmt.Errorf("null request body after gzip. msg:%v", requestContent), ""
	}

	httpReq, err := http.NewRequest("POST", postUrl, strings.NewReader(string(requestBody)))
	if err != nil {
		log.Errorf("NewRequest err:%v. msg:%v", err, requestContent)
		return err, ""
	}

	httpReq.Header.Set("header1", "polarbox-clustermanager;log;20200828001") //实例类型;数据类型;数据格式Schama版本号
	httpReq.Header.Set("header2", "auditlog;")                               //数据子类型
	httpReq.Header.Set("identityKey", identityKey)                           //"上报实例名;实例所在IP地址;实例所在端口号"
	if IsGzipBody {
		httpReq.Header.Set("Content-Encoding", "gzip")
		log.Infof("try post %v : head = %v , body:%v", postUrl, httpReq.Header, string(requestContent))
	} else {
		httpReq.Header.Set("Content-Type", "application/json")
		log.Infof("try post %v : head = %v , body:%v", postUrl, httpReq.Header, string(requestBody))
	}

	httpClient := &http.Client{Timeout: 10 * time.Second, Transport: redLineTransPort}
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		log.Errorf("Failed to request %s err %s", postUrl, err.Error())
	}

	if err != nil && httpResp == nil {
		return fmt.Errorf("Failed to request %s err %s", postUrl, err.Error()), ""
	}

	err, body := red.handleResponse(httpResp)
	log.Infof("post %v : head = %v , response body:%v , apiErr: %v", postUrl, httpReq.Header, body, err)
	if err != nil {
		log.Warnf("Failed to request %s body %s err %s", postUrl, body, err.Error())
	} else {
		return nil, body
	}
	return nil, ""

}

func (red *RedLineClient) VersionCheck() {
	if red.EndPoints == "" {
		return
	}
	newSupportEventApi := red.IsSupportV2Api()
	if newSupportEventApi != supportEventApi {
		log.Infof("support api switch from %v to %v", supportEventApi, newSupportEventApi)
		supportEventApi = newSupportEventApi
	}
}

func (red *RedLineClient) IsSupportV2Api() bool {
	err, ver := red.GetApiVersion()
	if err != nil {
		log.Warnf("try to get apiInfo err: %v, use default false", err)
		return false
	}
	log.Infof("get GetApiVersion :%s", ver)
	// //从4.5.0开始提供事件中心服务
	if "unknown" == ver || "" == ver {
		return false
	}
	verPars := strings.Split(ver, ".")
	p1 := verPars[0]
	p1Int, err := strconv.Atoi(p1)
	if err == nil {
		if p1Int > 4 { //第一个版本号大于4
			log.Infof("ver part 1 is %s, larger than 4, support event api!", p1)
			return true
		}
		if p1Int < 4 {
			log.Infof("ver part 1 is %s, smaller than 4, not support event api!", p1)
			return false
		}
		log.Infof("ver part 1 is %s, equal 4, so continue compare next parts", p1)
		//==4的情况，看到面版本的判断
	} else {
		//第1个版本号不能格式化为字符串，就按字符串大小比较
		log.Infof("ver part 1 is %v, not a number (err:%v), use string compare", p1, err)
		cValue := strings.Compare(p1, "4")
		if cValue > 0 {
			log.Info("ver part 1 is %v, not a number, string compare larger than 4, support event api", p1)
			return true
		}
		//与4相等的情况，在整数中就已覆盖，因此无需再次判断
		log.Infof("ver part 1 is %v, not a number, string compare smaller than 4, not support event api", p1)
		return false
	}

	if len(verPars) >= 2 {
		//第一步判断为相等，才会进入第二部判断
		p2 := verPars[1]
		p2Int, err := strconv.Atoi(p2)
		if err == nil {
			if p2Int >= 5 {
				log.Infof("ver part 2 is %s, larger than or equal 5, support event api!", p2)
				return true
			}
			log.Infof("ver part 2 is %s, smaller than  4, not support event api!", p2)
			return false
		} else {
			log.Infof("ver part 2 is %v, not a number (err:%v), use string compare", p2, err)
			cValue := strings.Compare(p1, "5")
			if cValue > 0 {
				log.Infof("ver part 2 is %v, not a number, string compare larger than 5, support event api", p2)
				return true
			}
			//与4相等的情况，在整数中就已覆盖，因此无需再次判断
			log.Infof("ver part 2 is %v, not a number, string compare smaller than 5, not support event api", p2)
			return false
		}
	}
	log.Infof("ver part check done, version: %s  ,use default false, not support event api", ver)

	return false
}

func (red *RedLineClient) GetApiVersion() (error, string) {
	postUrl := fmt.Sprintf("http://%v/master/redlineSimpleVersion", red.EndPoints)
	requestBody := "{}"
	httpReq, err := http.NewRequest("GET", postUrl, strings.NewReader(requestBody))
	if err != nil {
		log.Errorf("Failed to build request %s err %s", postUrl, err.Error())
		return fmt.Errorf("Failed to request %s err %s ", postUrl, err.Error()), ""
	}

	httpClient := &http.Client{Timeout: time.Duration(10 * time.Second)}
	log.Infof("try to send get request to %s, body: %s", postUrl, requestBody)
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		log.Errorf("Failed to request %s err %s", postUrl, err.Error())
		return fmt.Errorf("Failed to request %s err %s ", postUrl, err.Error()), ""
	}

	if err != nil && httpResp == nil {
		return fmt.Errorf("Failed to request %s err %s ", postUrl, err.Error()), ""
	}

	err, body := red.handleResponse(httpResp)
	if err != nil {
		log.Warnf("Failed to request %s body %s err %s", postUrl, body, err.Error())
		return fmt.Errorf("Failed to request %s body %s err %s ", postUrl, body, err.Error()), ""
	} else {
		log.Infof("try to send get request to %s, body: %s, response: %s", postUrl, requestBody, body)
		apiInfo := &ApiVersionBody{}
		uErr := json.Unmarshal([]byte(body), apiInfo)
		if uErr != nil {
			return fmt.Errorf("Failed to Unmarshal %s reponse body %s err %s ", postUrl, body, err.Error()), ""
		}
		if len(apiInfo.DataList) > 0 {
			return nil, apiInfo.DataList[0]
		}
		return nil, "unknown"
	}
}

func (red *RedLineClient) postV2(params interface{}, identityKey string) (error, string) {
	if red.EndPoints == "" {
		red.Init()
		supportEventApi = red.IsSupportV2Api()
	}
	if !supportEventApi {
		return fmt.Errorf("Event api not supported, skip postV2 execution，  and this msg be dropped:%v, iK:%s", params, identityKey), ""
	}

	jsonData, err := json.Marshal(params)
	if err != nil {
		return err, ""
	}
	requestContent := string(jsonData)

	//event center: doc: https://yuque.antfin-inc.com/aliyuntx/dg1dfo/wgg0os#985tb ,
	//share: https://yuque.antfin-inc.com/docs/share/1d964ca6-18cb-4f9b-bf22-147af9bee3dc?# 《事件中心技术方案设计》

	postUrl := fmt.Sprintf("http://%v/data/upload", red.EndPoints)
	requestBody := jsonData
	if IsGzipBody {
		requestBody = gzipContents(requestContent)
	}

	if requestBody == nil {
		log.Errorf("null request body after gzip. msg:%v", requestContent)
		return fmt.Errorf("null request body after gzip. msg:%v", requestContent), ""
	}

	httpReq, err := http.NewRequest("POST", postUrl, strings.NewReader(string(requestBody)))
	if err != nil {
		log.Errorf("NewRequest err:%v. msg:%v", err, requestContent)
		return err, ""
	}

	httpReq.Header.Set("header1", "event_resource;log;20210220001;POLAR_STACK") //实例类型;数据类型;数据格式Schama版本号
	httpReq.Header.Set("header2", "event;")                                     //数据子类型
	httpReq.Header.Set("identityKey", identityKey)                              //"上报实例名;实例所在IP地址;实例所在端口号"
	if IsGzipBody {
		httpReq.Header.Set("Content-Encoding", "gzip")
		log.Infof("try postV2 %v : head = %v , body:%v", postUrl, httpReq.Header, string(requestContent))
	} else {
		httpReq.Header.Set("Content-Type", "application/json")
		log.Infof("try postV2 %v : head = %v , body:%v", postUrl, httpReq.Header, string(requestBody))
	}

	httpClient := &http.Client{Timeout: time.Duration(10 * time.Second)}
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		log.Errorf("Failed to request %s err %s", postUrl, err.Error())
	}

	if err != nil && httpResp == nil {
		return fmt.Errorf("Failed to request %s err %s ", postUrl, err.Error()), ""
	}

	err, body := red.handleResponse(httpResp)
	log.Infof("postV2 %v : head = %v , response body:%v , apiErr: %v", postUrl, httpReq.Header, body, err)
	if err != nil {
		log.Warnf("Failed to request %s body %s err %s", postUrl, body, err.Error())
	} else {

		return nil, body
	}
	return nil, ""

}

func (red *RedLineClient) handleResponse(resp *http.Response) (error, string) {
	if resp == nil {
		return nil, ""
	}
	defer func() {
		e := resp.Body.Close()
		if e != nil {
			//
		}
	}()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err, string(body)
	}

	return nil, string(body)
}

func (red *RedLineClient) SendBoxInsEvent(msg map[string]interface{}) error {
	err, errMsg := red.post(msg)
	if err != nil {
		log.Warnf("Failed to SendBoxInsEvent err:%v   msg:%s", err, errMsg)
		return err
	}
	return nil
}

func (red *RedLineClient) SendBoxInsEventV2(msg interface{}, idKey string) error {
	err, errMsg := red.postV2(msg, idKey)
	if err != nil {
		log.Warnf("Failed to SendBoxInsEventV2 err:%v msg:%s", err, errMsg)
		return err
	}
	return nil
}

func gzipContents(msg string) []byte {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write([]byte(msg)); err != nil {
		log.Errorf(" gzip compress msg err: %v, orgMsg: %v", err, msg)
		return nil
	}
	if err := gz.Flush(); err != nil {
		log.Errorf(" gzip compress msg err: %v, orgMsg: %v", err, msg)
		return nil
	}
	if err := gz.Close(); err != nil {
		log.Errorf(" gzip compress msg err: %v, orgMsg: %v", err, msg)
		return nil
	}
	return b.Bytes()
}
