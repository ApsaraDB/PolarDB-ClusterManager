package collector

import (
	"encoding/json"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	EngineAliveEvent string = "EngineAliveEvent"
	EngineFailEvent  string = "EngineFailEvent"
)

type LogagentCollector struct {
	clusterID      string
	InsSpec        *common.InsSpec
	StopFlag       chan bool
	StopCollect    chan bool
	Started        bool
	EventCallbacks []common.EventHandler
	eventQueue     chan common.Event
}

type CollectRequest struct {
	InsID string `json:"ins_id"`
}

type CollectResponse struct {
	ErrCode int                    `json:"err_code"`
	ErrMsg  string                 `json:"err_msg"`
	Metrics map[string]interface{} `json:"metrics"`
}

type StorageUsageEvent struct {
	common.BaseEvent
}

func (StorageUsageEvent) EventCategory() common.EventCategory {
	return common.StorageUsageEventCategory
}

type PodEngineEvent struct {
	common.BaseEvent
}

func (e *PodEngineEvent) EventCategory() common.EventCategory {
	return common.PodEngineEventCategory
}

type PodMetricsEvent struct {
	common.BaseEvent
}

func (e *PodMetricsEvent) EventCategory() common.EventCategory {
	return common.PodMetricsEventCategory
}

func NewLogAgentCollector(clusterID string, insSpec *common.InsSpec) *LogagentCollector {
	return &LogagentCollector{
		clusterID:   clusterID,
		InsSpec:     insSpec,
		eventQueue:  make(chan common.Event, 30),
		StopFlag:    make(chan bool, 1),
		StopCollect: make(chan bool),
		Started:     false,
	}
}

func (c *LogagentCollector) ID() string {
	return "logagentCollector@" + c.InsSpec.String()
}

func (c *LogagentCollector) ClusterID() string {
	return c.clusterID
}

func (c *LogagentCollector) EndPoint() common.EndPoint {
	return c.InsSpec.Endpoint
}

func (c *LogagentCollector) Start() error {
	if resource.IsPolarBoxOneMode() {
		_, err := resource.GetResourceManager().GetK8sClient().GetInsInfoFromK8s(c.InsSpec.PodName, c.InsSpec)
		if err != nil {
			log.Warnf("Failed to get ins %s from k8s err %s", c.InsSpec.String(), err.Error())
		}
	}
	if c.Started {
		return nil
	}
	log.Infof("Starting LogAgent collect %s", c.InsSpec.String())
	go func() {
		defer log.Infof("LogAgentCollector %s:%s quit.", c.clusterID, c.InsSpec.String())
		for {
			select {
			case ev := <-c.eventQueue:
				for _, callback := range c.EventCallbacks {
					err := callback.HandleEvent(ev)
					if err != nil {
						log.Errorf(" LogagentCollector exec %v callBack event: %v err: %v", callback.Name(), common.EnvSimpleName(ev), err)
					}
				}
			case <-c.StopFlag:
				c.StopCollect <- true
				return
			}
		}
	}()
	c.Started = true
	go c.Collect()

	return nil
}

func (c *LogagentCollector) Stop() error {
	c.StopFlag <- true
	return nil
}

func (c *LogagentCollector) ReStart() error {
	if resource.IsPolarBoxOneMode() {
		_, err := resource.GetResourceManager().GetK8sClient().GetInsInfoFromK8s(c.InsSpec.PodName, c.InsSpec)
		if err != nil {
			log.Warnf("Failed to get ins %s from k8s err %s", c.InsSpec.String(), err.Error())
		}
	}
	log.Infof("ReStarting LogAgent collect %s", c.InsSpec.String())
	return nil
}

func (c *LogagentCollector) Collect() error {
	errTimes := 0
	lastErr := ""
	for {
		select {
		case <-c.StopCollect:
			log.Infof("LogAgentCollector %s collector routine stop!", c.InsSpec)
			return nil
		case <-time.After(time.Millisecond):
			break
		}
		startTs := time.Now()
		url := "/v1/metrics"
		var param CollectRequest
		if resource.IsPolarPureMode() {
			param.InsID = c.InsSpec.Endpoint.String()
		} else {
			param.InsID = c.InsSpec.ID
		}
		err, metrics := c.CollectMetrics(url, &param)
		if err != nil {
			if err.Error() != lastErr {
				log.Warnf("Failed to Collect LogAgent %s with %v", c.InsSpec.String(), err)
				errTimes = 0
			} else if errTimes < 10 || errTimes%300 == 0 {
				log.Warnf("Failed to Collect LogAgent %s with %v", c.InsSpec.String(), err)
			}
			lastErr = err.Error()
			errTimes++
			if strings.Contains(err.Error(), "timeout") ||
				strings.Contains(err.Error(), "Timeout") ||
				strings.Contains(err.Error(), "no route to host") {
				c.eventQueue <- &PodCpuEvent{
					common.BaseEvent{
						EvName:      ContainerTimeoutEvent,
						EvTimestamp: time.Now(),
						InsID:       c.InsSpec.ID,
					}}
			} else {
				c.eventQueue <- &PodCpuEvent{
					common.BaseEvent{
						EvName:      ContainerUnknownEvent,
						EvTimestamp: time.Now(),
						InsID:       c.InsSpec.ID,
					}}
			}
		} else {
			if time.Since(startTs) > time.Second {
				log.Infof("Collect %s long time %s", url, time.Since(startTs).String())
			}
			c.HandleCpuMetrics(metrics)
			if resource.IsPolarBoxMode() {
				c.HandleEngineStatusMetrics(metrics)
			} else {
				c.HandlePfsUsageMetrics(metrics)
			}

			c.HandlePodMetrics(metrics)
		}

		elapsedTime := time.Since(startTs)
		if elapsedTime < (time.Duration(*common.EngineMetricsIntervalMs) * time.Millisecond) {
			time.Sleep((time.Duration(*common.EngineMetricsIntervalMs) * time.Millisecond) - elapsedTime)
		}
	}

	return nil
}

func (c *LogagentCollector) CollectMetrics(req string, params *CollectRequest) (error, map[string]interface{}) {
	jsonData, err := json.Marshal(params)
	if err != nil {
		return err, nil
	}
	requestContent := string(jsonData)

	url := ""
	if resource.IsPolarBoxMode() {
		url = "http://" + c.InsSpec.HostName + ":" + strconv.Itoa(resource.GetClusterManagerConfig().UE.MetricsPort) + req
	} else {
		url = "http://" + c.InsSpec.Endpoint.Host + ":" + strconv.Itoa(resource.GetClusterManagerConfig().UE.MetricsPort) + req
	}
	httpReq, err := http.NewRequest("POST", url, strings.NewReader(requestContent))
	httpReq.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: time.Duration(*common.EngineMetricsTimeoutMs) * time.Millisecond}
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		return err, nil
	}

	err, body := c.handleReponse(httpResp)
	if err != nil {
		return err, nil
	}
	response := &CollectResponse{}
	if err := json.Unmarshal(body, response); err != nil {
		return errors.Wrapf(err, "error unmarshal response %s", string(body)), nil
	}

	if response.ErrCode != 200 {
		return errors.New("response status:" + response.ErrMsg), nil
	}

	return nil, response.Metrics
}

func (c *LogagentCollector) handleReponse(resp *http.Response) (error, []byte) {
	statusCode := resp.StatusCode

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err, body
	}

	if statusCode != 200 {
		return errors.New(string(body)), body
	}

	return nil, body
}

func (c *LogagentCollector) HandlePodMetrics(metrics map[string]interface{}) {

	c.eventQueue <- &PodMetricsEvent{
		BaseEvent: common.BaseEvent{
			EvName:      PodMetricsCollectEvent,
			EvTimestamp: time.Now(),
			InsID:       c.InsSpec.ID,
			Values:      metrics,
		},
	}
}

func (c *LogagentCollector) HandleCpuMetrics(metrics map[string]interface{}) {
	v, exist := metrics["cpu_total"]
	cpuMetrics := map[string]interface{}{}
	if exist {
		vs, ok := v.(string)
		if ok {
			usage, err := strconv.Atoi(vs)
			if err == nil {
				cpuMetrics["cpu_total"] = v
				if usage > (UsageThreshold * 100) {
					c.eventQueue <- &PodCpuEvent{
						common.BaseEvent{
							EvName:      ContainerCpuFullEvent,
							InsID:       c.InsSpec.ID,
							EvTimestamp: time.Now(),
							Values:      cpuMetrics,
						}}
				} else if usage < (EmptyPercentage * 100) {
					c.eventQueue <- &PodCpuEvent{
						common.BaseEvent{
							EvName:      ContainerCpuEmptyEvent,
							InsID:       c.InsSpec.ID,
							EvTimestamp: time.Now(),
							Values:      cpuMetrics,
						}}
				} else {
					c.eventQueue <- &PodCpuEvent{
						common.BaseEvent{
							EvName:      ContainerCpuNormalEvent,
							InsID:       c.InsSpec.ID,
							EvTimestamp: time.Now(),
							Values:      cpuMetrics,
						}}
				}
				return
			}
		}
	}
	c.eventQueue <- &PodCpuEvent{
		common.BaseEvent{
			EvName:      ContainerUnknownEvent,
			InsID:       c.InsSpec.ID,
			EvTimestamp: time.Now(),
		}}
}

func (c *LogagentCollector) HandlePfsUsageMetrics(metrics map[string]interface{}) {
	hasUsage := false
	for k, v := range metrics {
		if k != "pls_inode_usage" && k != "pls_direntry_usage" && k != "pls_blk_usage" {
			continue
		}
		vs, ok := v.(string)
		if !ok {
			log.Warnf("Invalid metrics %s:%v", k, v)
			continue
		}
		usage, err := strconv.Atoi(vs)
		if err == nil {
			if usage >= (UsageThreshold * 100) {
				log.Infof("HandlePfsUsageMetrics: %v = %v, trigger: StorageUsageFullEvent[%v]", k, v, UsageThreshold)
				c.eventQueue <- &StorageUsageEvent{
					common.BaseEvent{
						EvName:      StorageUsageFullEvent,
						InsID:       c.InsSpec.ID,
						EvTimestamp: time.Now(),
						Values:      metrics,
					}}
				return
			} else {
				hasUsage = true
			}
		} else {
			log.Warnf("HandlePfsUsageMetrics: Invalid usage %s:%v", k, v)
			continue
		}
	}
	if hasUsage {
		c.eventQueue <- &StorageUsageEvent{
			common.BaseEvent{
				EvName:      StorageUsageNormalEvent,
				InsID:       c.InsSpec.ID,
				EvTimestamp: time.Now(),
			}}
	} else {
		c.eventQueue <- &StorageUsageEvent{
			common.BaseEvent{
				EvName:      ContainerUnknownEvent,
				InsID:       c.InsSpec.ID,
				EvTimestamp: time.Now(),
			}}
	}
}

func (c *LogagentCollector) HandleEngineStatusMetrics(metrics map[string]interface{}) {
	v, exist := metrics["polarbox_engine_status"]
	if exist {
		st, ok := v.(string)
		if ok {
			if st == "alive" {
				c.eventQueue <- &PodEngineEvent{
					BaseEvent: common.BaseEvent{
						EvName:      EngineAliveEvent,
						InsID:       c.InsSpec.ID,
						EvTimestamp: time.Now()},
				}
			} else {
				hasDetailError := false
				for _, reason := range common.DetailFailReason {
					if strings.Contains(st, reason) {
						log.Infof("HandleEngineStatusMetrics: %v Contains %v, trigger: %v", st, reason, EngineFailEvent)
						c.eventQueue <- &PodEngineEvent{
							BaseEvent: common.BaseEvent{
								EvName: EngineFailEvent,
								InsID:  c.InsSpec.ID,
								Values: map[string]interface{}{
									common.EventReason: reason,
								},
								EvTimestamp: time.Now()},
						}
						hasDetailError = true
					}
				}
				if !hasDetailError {
					log.Infof("HandleEngineStatusMetrics: %v hasDetailError= %v, trigger: %v", st, hasDetailError, EngineFailEvent)
					c.eventQueue <- &PodEngineEvent{
						BaseEvent: common.BaseEvent{
							EvName: EngineFailEvent,
							InsID:  c.InsSpec.ID,
							Values: map[string]interface{}{
								common.EventReason: st,
							},
							EvTimestamp: time.Now()},
					}
				}
			}
			return
		}
	}
	c.eventQueue <- &PodEngineEvent{
		BaseEvent: common.BaseEvent{
			EvName: EngineFailEvent,
			InsID:  c.InsSpec.ID,
			Values: map[string]interface{}{
				common.EventReason: "no pod engine status from universe",
			},
			EvTimestamp: time.Now()},
	}
}

func (c *LogagentCollector) RegisterEventCallback(callback common.EventHandler) error {
	c.EventCallbacks = append(c.EventCallbacks, callback)
	return nil
}
