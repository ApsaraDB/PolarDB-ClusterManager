package collector

import (
	"context"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/polarbox_hardware_service"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"

	"google.golang.org/grpc"
)

type HardwareServiceCollector struct {
	InsSpec        *common.InsSpec
	StopFlag       chan bool
	StopCollect    chan bool
	Started        bool
	EventCallbacks []common.EventHandler
	eventQueue     chan common.Event
	errTimes       uint
	reqTimes       uint
	lastErr        string
	sleepTimes     time.Duration
}

func NewHardwareServiceCollector(insSpec *common.InsSpec) *HardwareServiceCollector {
	return &HardwareServiceCollector{
		InsSpec:     insSpec,
		eventQueue:  make(chan common.Event, 30),
		StopFlag:    make(chan bool, 1),
		StopCollect: make(chan bool),
		Started:     false,
	}
}

func (c *HardwareServiceCollector) ID() string {
	return "logagentCollector@" + c.InsSpec.String()
}

func (c *HardwareServiceCollector) ClusterID() string {
	return c.InsSpec.ClusterID
}

func (c *HardwareServiceCollector) EndPoint() common.EndPoint {
	return c.InsSpec.Endpoint
}

func (c *HardwareServiceCollector) Start() error {
	if resource.IsPolarBoxMode() {
		_, err := resource.GetResourceManager().GetK8sClient().GetInsInfoFromK8s(c.InsSpec.PodName, c.InsSpec)
		if err != nil {
			log.Warnf("Failed to get ins %s from k8s err %s", c.InsSpec.String(), err.Error())
		}
	}
	if c.Started {
		return nil
	}
	log.Infof("Starting collect hardware service %s", c.InsSpec.String())
	go func() {
		defer log.Infof("HardwareServiceCollector %s quit.", c.InsSpec.String())
		for {
			select {
			case ev := <-c.eventQueue:
				for _, callback := range c.EventCallbacks {
					err := callback.HandleEvent(ev)
					if err != nil {
						log.Errorf(" HardwareServiceCollector exec %v callBack event: %v err: %v", callback.Name(), common.EnvSimpleName(ev), err)
					}
				}
			case <-c.StopFlag:
				c.StopCollect <- true
				return
			}
		}
	}()
	go c.Collect()
	c.Started = true

	return nil
}

func (c *HardwareServiceCollector) Stop() error {
	c.StopFlag <- true
	return nil
}

func (c *HardwareServiceCollector) ReStart() error {
	if resource.IsPolarBoxMode() {
		_, err := resource.GetResourceManager().GetK8sClient().GetInsInfoFromK8s(c.InsSpec.PodName, c.InsSpec)
		if err != nil {
			log.Warnf("Failed to get ins %s from k8s err %s", c.InsSpec.String(), err.Error())
		}
	}
	log.Infof("ReStarting collect hardware service %s", c.InsSpec.String())
	return nil
}

func (c *HardwareServiceCollector) Collect() error {
	c.sleepTimes = time.Duration(1)

	for {
		select {
		case <-time.After(HardwareCollectorInterval * c.sleepTimes):
			break
		case <-c.StopCollect:
			log.Infof("HardwareServiceCollector %s stop!", c.InsSpec)
			return nil
		}

		c.sleepTimes = time.Duration(1)

		c.Request()

		c.reqTimes++
	}

	return nil
}

func (c *HardwareServiceCollector) RegisterEventCallback(callback common.EventHandler) error {
	c.EventCallbacks = append(c.EventCallbacks, callback)
	return nil
}

func (c *HardwareServiceCollector) Request() error {

	conn, err := grpc.Dial("localhost:30003", grpc.WithInsecure())
	if err != nil {
		log.Warnf("Failed to connect to hardware service err=%s", err.Error())
		return err
	}
	defer conn.Close()
	startTs := time.Now()
	client, _, _ := resource.GetDefaultKubeClient()

	ctx, _ := context.WithTimeout(context.Background(), HardwareCollectorInterval)
	resp, err := polarbox_hardware_service.NewHardwareClient(conn).GetDBServerStatus(
		ctx, &polarbox_hardware_service.GetDBServerStatusRequest{Hostname: c.InsSpec.HostName})
	if err != nil {
		if c.errTimes%10 == 0 || err.Error() != c.lastErr {
			log.Warnf("Failed to Collect Hardware Service %s with %v", c.InsSpec.String(), err)
		}
		c.lastErr = err.Error()
		c.errTimes++
		c.eventQueue <- &EngineConfEvent{
			BaseEvent: common.BaseEvent{
				EvName:      EngineMetricsCollectEvent,
				EvTimestamp: time.Now(),
				InsID:       c.InsSpec.ID,
				Values: map[string]interface{}{
					common.EventServerStatus: common.ServerStatusOn,
				},
			},
			Category: common.EngineMetricsEventCategory,
		}
		return err
	} else if c.lastErr != "" || c.reqTimes == 0 {
		ts := time.Unix(resp.Timestamp.Seconds, int64(resp.Timestamp.Nanos))
		log.Infof("Success to req hostname %s resp %s ts %s", c.InsSpec.HostName, resp.Status, ts.String())
		c.lastErr = ""
	}
	if time.Since(startTs) > time.Second {
		log.Info("Collect long time", time.Since(startTs))
	}
	if resp.Status == common.ServerStatusOff {
		ts := time.Unix(resp.Timestamp.Seconds, int64(resp.Timestamp.Nanos))
		log.Infof("req hostname %s resp %s ts %s", c.InsSpec.HostName, resp.Status, ts.String())
		if resource.IsPolarBoxOneMode() {
			pod, err := client.CoreV1().Pods(resource.GetClusterManagerConfig().Cluster.Namespace).Get(c.InsSpec.PodName, metav1.GetOptions{})
			if err != nil {
				log.Warnf("Failed to get ins %s local hostname err %s", c.InsSpec.String(), err.Error())
				return err
			}
			if pod.Spec.NodeName != c.InsSpec.HostName {
				log.Warnf("Req host %s actual host %s", c.InsSpec.HostName, pod.Spec.Hostname)
				return errors.Errorf("Req host %s actual host %s", c.InsSpec.HostName, pod.Spec.Hostname)
			}
		}
		c.sleepTimes = time.Duration(10)
		if time.Since(ts) > time.Duration(3)*time.Second {
			resp.Status = common.ServerStatusOn
			log.Warnf("server status has %s delay skip it", time.Since(ts).String())
		}
	}
	c.eventQueue <- &EngineConfEvent{
		BaseEvent: common.BaseEvent{
			EvName:      EngineMetricsCollectEvent,
			EvTimestamp: time.Now(),
			InsID:       c.InsSpec.ID,
			Values: map[string]interface{}{
				common.EventServerStatus: resp.Status,
			},
		},
		Category: common.EngineMetricsEventCategory,
	}

	return nil
}
