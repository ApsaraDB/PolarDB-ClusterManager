package resource

import (
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type OperatorClientInterface interface {
	// conf
	LoadConf(v []byte) error
	GetType() string

	// subscribe
	MasterReplicaMetaSwitch(oldRw common.InsSpec, newRw common.InsSpec, clusterID string) error
	MasterStandbyMetaSwitch(oldRw common.InsSpec, newRw common.InsSpec) error
	UpdateEngineStatus(ins common.InsSpec, state, reason string) error
	ReadOnlyLock(lock bool) error

	// service
	SwitchVip(from common.InsSpec, to common.InsSpec, switchType string) (error, string)
	SwitchStore(from common.InsSpec, to common.InsSpec) (error, string)
}

type DefaultOperatorClient struct {
}

func (c *DefaultOperatorClient) GetType() string {
	return "default"
}

func (c *DefaultOperatorClient) LoadConf(conf []byte) error {
	return nil
}

func (c *DefaultOperatorClient) SwitchVip(from common.InsSpec, to common.InsSpec, switchType string) (error, string) {
	start := time.Now()
	err := GetResourceManager().GetVipManager().SwitchVip(&from, &to)

	l := fmt.Sprintf("switch rw vip backend from %s to %s cost:%s.",
		from.String(), to.String(), time.Since(start).String())

	return err, l
}

func (c *DefaultOperatorClient) SwitchStore(from common.InsSpec, to common.InsSpec) (error, string) {
	return nil, ""
}

func (c *DefaultOperatorClient) MasterReplicaMetaSwitch(oldRw common.InsSpec, newRw common.InsSpec, clusterID string) error {
	return nil
}

func (c *DefaultOperatorClient) MasterStandbyMetaSwitch(oldRw common.InsSpec, newRw common.InsSpec) error {
	return nil
}

func (c *DefaultOperatorClient) UpdateEngineStatus(ins common.InsSpec, state, reason string) error {
	return nil
}

func (c *DefaultOperatorClient) ReadOnlyLock(lock bool) error {
	return nil
}

type Response struct {
	Status string                 `json:"status,omitempty"`
	Code   int                    `json:"code"`
	Msg    map[string]interface{} `json:"msg,omitempty"`
}

type UpdateEndpointCallback func() []common.EndPoint

type EndpointHttpClient struct {
	Endpoint        []common.EndPoint
	RetryTimes      int
	Timeout         int
	lastEpCheckTime time.Time
	UpdateEndpoint  UpdateEndpointCallback
}

func UpdateK8sOperatorEndpoint() []common.EndPoint {
	eps, _, err := GetResourceManager().GetK8sClient().GetOperatorEndpoints()
	if err != nil {
		log.Warnf("Failed to update operator endpoints err %s", err.Error())
		return nil
	}

	return eps
}

func (c *EndpointHttpClient) ensureUpdateEndpoint() {
	if c.UpdateEndpoint == nil {
		return
	}
	resSec := time.Now().Sub(c.lastEpCheckTime).Seconds()
	if resSec > 10 { //超过10秒，也就是避免在一次循环中，多次调用k8s api查询管控地址
		eps := c.UpdateEndpoint()
		if eps != nil {
			c.lastEpCheckTime = time.Now()
			c.Endpoint = eps
			log.Infof("Get Operator api eps: %v", eps)
		}
	}

	if c.Endpoint == nil || len(c.Endpoint) < 1 {
		eps := c.UpdateEndpoint()
		if eps != nil {
			c.lastEpCheckTime = time.Now()
			c.Endpoint = eps
			log.Infof("Get Operator api eps: %v", eps)
		}
		return
	}
}
func (c *EndpointHttpClient) PostRequest(req string, params map[string]interface{}, handleRes bool) (error, string) {
	return c.Request(req, params, handleRes, "POST")
}

func (c *EndpointHttpClient) GetRequest(req string, params map[string]interface{}, handleRes bool) (error, string) {
	return c.Request(req, params, handleRes, "GET")
}

func (c *EndpointHttpClient) Request(req string, params map[string]interface{}, handleRes bool, method string) (error, string) {

	retryTimes := c.RetryTimes
	if c.RetryTimes < 1 {
		retryTimes = 1
	}

	for tryTimes := 0; tryTimes <= retryTimes; tryTimes++ {

		reqId := buildCmRequestId()

		newParams := make(map[string]interface{})
		newParams["cmReqId"] = reqId
		for k, v := range params {
			newParams[k] = v
		}

		log.Infof("operatorApiRequest: tryTime=%v, req=%v,param= %v", tryTimes, req, newParams)
		c.ensureUpdateEndpoint()
		body, errMsg := tryRequestOnce(c.Endpoint, req, newParams, handleRes, method)
		if errMsg == nil {
			return nil, body
		}
		log.Errorf("operatorApiRequest err: tryTime=%v ,  req=%v,param= %v , eps:%v , err:%v", tryTimes, req, newParams, c.Endpoint, errMsg)
		time.Sleep(time.Duration(tryTimes) * time.Second)
	}

	return fmt.Errorf("Failed to request operator, all endpoint result is invalid![times:%v] ", retryTimes), ""
}

func tryRequestOnce(endpoints []common.EndPoint, req string, params map[string]interface{}, handleRes bool, method string) (string, error) {
	if endpoints == nil || len(endpoints) < 1 {
		return "", errors.Errorf("tryRequestOnce: ep is nil")
	}

	epNum := len(endpoints)

	resultChan := make(chan string, 1)
	errMsgArray := make(chan error, 3)

	lock := sync.Mutex{}
	hasRequestSuccess := false

	defer func() {
		close(resultChan)
		close(errMsgArray)
	}()

	order := 0
	for _, ep := range endpoints {
		sleepTime := order * 500
		order++
		go func(_ep common.EndPoint, _req string, _params map[string]interface{}, _sleepTimeMs int, method string) {
			if _sleepTimeMs > 0 {
				time.Sleep(time.Duration(_sleepTimeMs) * time.Millisecond)
			}
			if hasRequestSuccess { //如已经成功，则不再重复request
				log.Infof("tryRequestOnce: Success to request %s : url %v , [hasRequestSuccess=%v] params: %v result: has requestSuccess,skip it. order=%v", _ep.String(), req, hasRequestSuccess, _params, _sleepTimeMs)
				return
			}

			bodyInfo, err := httpRequest(_ep, _req, _params, handleRes, method)

			lock.Lock()
			defer lock.Unlock()

			if err != nil {
				log.Warnf("tryRequestOnce: Failed to request %s error : %s", _ep.String(), err.Error())
				if !hasRequestSuccess {
					errMsgArray <- fmt.Errorf("tryRequestOnce: Failed to request %s error: %s ", _ep.String(), err.Error())
				}
				return
			}
			log.Infof("tryRequestOnce: Success to request %s : url %v , [hasRequestSuccess=%v] params: %v result: %v", _ep.String(), req, hasRequestSuccess, _params, bodyInfo)
			if !hasRequestSuccess {
				resultChan <- bodyInfo
			}
			hasRequestSuccess = true
		}(ep, req, params, sleepTime, method)
	}

	errNum := 0

	for i := 0; i < epNum; i++ {
		select {
		case errMsg := <-errMsgArray:
			errNum++
			log.Errorf("tryRequestOnce: Get a errMsg: %v", errMsg)
		case res := <-resultChan:
			//正常结果退出
			log.Infof("--Success-tryRequestOnce: request url %v , post: %v result: %v", req, hasRequestSuccess, res)
			return res, nil
		}
	}

	if errNum >= epNum {
		return "", errors.Errorf("tryRequestOnce：all ep request failed")
	}
	return "", nil
}

var requestCnt uint64 = 0

const requestCntMax uint64 = 99999999999999999

func buildCmRequestId() string {
	now := time.Now()
	nowStr := now.Format("2006-01-02T15-04-05")
	hostName, _ := os.Hostname()
	rd := rand.New(rand.NewSource(now.UnixNano()))
	saltValue := rd.Uint64()
	if requestCnt >= requestCntMax {
		requestCnt = 0
	}
	requestCnt++

	return fmt.Sprintf("CM%v-%s-%v-%v-[%s]", requestCnt, nowStr, now.Nanosecond(), saltValue, hostName)
}

func httpRequest(_ep common.EndPoint, _req string, params map[string]interface{}, handleRes bool, method string) (string, error) {
	url := "http://" + _ep.String() + _req
	log.Infof("request url: %v params %v", url, params)
	var httpReq *http.Request
	if method == "POST" {
		jsonData, err := json.Marshal(params)
		if err != nil {
			return "", err
		}
		requestContent := string(jsonData)

		httpReq, err = http.NewRequest("POST", url, strings.NewReader(requestContent))
	} else if method == "GET" {
		if len(params) > 0 {
			url = url + "?"
			for k, v := range params {
				url += fmt.Sprintf("%s=%v&", k, v)
			}
			url = url[:len(url)-1]
		}
		httpReq, _ = http.NewRequest("GET", url, nil)
	} else {
		return "", errors.Errorf("unsupport method %s", method)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: time.Duration(20 * time.Second)}

	httpResp, err := httpClient.Do(httpReq)

	defer func() {
		if httpResp != nil && httpResp.Body != nil {
			e := httpResp.Body.Close()
			if e != nil {
				//
			}
		}
	}()

	if err != nil {
		log.Warnf("Failed to request %s err %s", _ep.String(), err.Error())
		return "", err
	}

	if !handleRes {
		body, err := ioutil.ReadAll(httpResp.Body)
		if httpResp.StatusCode == 200 {
			return string(body), err
		} else {
			return string(body), errors.New(string(body))
		}
	}

	err, body := handleResponse(httpResp)
	if err != nil {
		log.Warnf("Failed to request %s body %s err %s", _ep.String(), body, err.Error())
		return "", err
	}

	return body, nil
}

func handleResponse(resp *http.Response) (error, string) {
	statusCode := resp.StatusCode

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err, string(body)
	}

	if statusCode != 200 {
		return errors.New(string(body)), string(body)
	}
	response := &Response{}
	if err := json.Unmarshal(body, response); err != nil {
		return errors.Wrap(err, "error unmarshal response"), string(body)
	}

	if response.Status != "ok" && response.Code != 200 {
		return errors.New("response status:" + response.Status + " code:" + strconv.Itoa(response.Code)), string(body)
	}

	return nil, string(body)
}
