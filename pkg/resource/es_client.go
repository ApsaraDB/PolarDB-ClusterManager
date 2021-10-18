package resource

import (
	"encoding/json"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type EsClientResponse struct {
	Status string                 `json:"status,omitempty"`
	Msg    map[string]interface{} `json:"msg,omitempty"`
}

type EsClient struct {
	EsEndpoint []common.EndPoint
}

func (c *EsClient) UpdateEndpoint() {
	eps, err := GetResourceManager().GetK8sClient().GetEsEndpoints()
	if err != nil {
		log.Warnf("Failed to update es endpoints err %s", err.Error())
		return
	}
	c.EsEndpoint = eps
}

func (c *EsClient) request(req string, params map[string]interface{}) (error, string) {
	jsonData, err := json.Marshal(params)
	if err != nil {
		return err, ""
	}
	requestContent := string(jsonData)

	updateEndpoint := false
	for {
		for _, ep := range c.EsEndpoint {
			url := "http://" + ep.String() + req
			log.Infof("request url: %v requestContent %v", url, requestContent)
			httpReq, err := http.NewRequest("POST", url, strings.NewReader(requestContent))
			httpReq.Header.Set("Content-Type", "application/json")

			httpClient := &http.Client{Timeout: time.Duration(10 * time.Second)}
			httpResp, err := httpClient.Do(httpReq)
			if err != nil {
				log.Warnf("Failed to request %s err %s", ep.String(), err.Error())
				continue
			}

			err, body := c.handleReponse(httpResp)
			if err != nil {
				log.Warnf("Failed to request %s body %s err %s", ep.String(), body, err.Error())
				continue
			} else {
				return err, body
			}
		}
		if updateEndpoint {
			break
		}
		c.UpdateEndpoint()
		updateEndpoint = true
	}

	return errors.New("Failed to request operator, all endpoint is invalid!"), ""
}

func (c *EsClient) handleReponse(resp *http.Response) (error, string) {
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err, string(body)
	}

	return nil, string(body)
}

func (c *EsClient) SendBoxInsEvent(msg map[string]interface{}) error {
	url := "/polarbox-instance-event-" + time.Now().Format("2006-01-02") + "/_doc"
	err, errMsg := c.request(url, msg)
	if err != nil {
		log.Warnf("Failed to SendBoxInsEvent err:%v msg:%s", err, errMsg)
		return err
	}
	return nil
}
