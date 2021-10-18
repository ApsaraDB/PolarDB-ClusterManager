package collector

import (
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"strings"
	"time"
)

const (
	PodUnknownEvent       = "PodUnknownEvent"
	PodEngineRestartEvent = "PodEngineRestartEvent"
	PodEngineRunningEvent = "PodEngineRunningEvent"

	PodNetworkBond1UpEvent   = "PodNetworkBond1UpEvent"
	PodNetworkBond1DownEvent = "PodNetworkBond1DownEvent"

	PodCollectorEventProducer = "PodCollectorEventProducer"
)

type PodCollector struct {
	clusterID      string
	InsSpec        common.InsSpec
	StopFlag       chan bool
	StopCollect    chan bool
	Started        bool
	RestartCount   int32
	EventCallbacks []common.EventHandler
	eventQueue     chan common.Event
}

type PodEvent struct {
	common.BaseEvent
}

func (e *PodEvent) EventCategory() common.EventCategory {
	return common.PodEventCategory
}

type PodNetworkEvent struct {
	common.BaseEvent
}

func (e *PodNetworkEvent) EventCategory() common.EventCategory {
	return common.PodNetworkEventCategory
}

func NewPodCollector(clusterID string, insSpec *common.InsSpec) *PodCollector {
	log.Infof("Starting pod collector %s", insSpec.String())
	return &PodCollector{
		clusterID:    clusterID,
		InsSpec:      *insSpec,
		RestartCount: 0,
		eventQueue:   make(chan common.Event, 30),
		StopFlag:     make(chan bool, 1),
		StopCollect:  make(chan bool),
	}
}

func (c *PodCollector) ID() string {
	return "collector@" + c.InsSpec.String()
}

func (c *PodCollector) Type() string {
	return PodCollectorEventProducer
}

func (c *PodCollector) ClusterID() string {
	return c.clusterID
}

func (c *PodCollector) EndPoint() common.EndPoint {
	return c.InsSpec.Endpoint
}

func (c *PodCollector) Start() error {
	if c.Started {
		return nil
	}
	go c.Collect()

	go func() {
		defer log.Infof("PodCollector %s quit.", c.InsSpec.String())
		for {
			select {
			case ev := <-c.eventQueue:
				for _, callback := range c.EventCallbacks {
					err := callback.HandleEvent(ev)
					if err != nil {
						log.Errorf(" PodCollector exec %v callBack event: %v err: %v", callback.Name(), common.EnvSimpleName(ev), err)
					}
				}
			case <-c.StopFlag:
				c.StopCollect <- true
				return
			}
		}
	}()
	c.Started = true

	return nil
}

func (c *PodCollector) Stop() error {
	c.StopFlag <- true
	return nil
}

func (c *PodCollector) ReStart() error {
	return nil
}

func (c *PodCollector) CollectPodRestart() {
	client := resource.GetResourceManager().GetK8sClient()

	pod, err := client.GetPodInfo(c.InsSpec.PodName)
	if err != nil {
		c.eventQueue <- &PodEvent{common.BaseEvent{EvName: PodUnknownEvent, InsID: c.InsSpec.ID, EvTimestamp: time.Now()}}
	} else {
		for _, s := range pod.Status.ContainerStatuses {
			if s.Name == "engine" {
				if s.RestartCount != c.RestartCount {
					c.RestartCount = s.RestartCount
					c.eventQueue <- &PodEvent{common.BaseEvent{EvName: PodEngineRestartEvent, InsID: c.InsSpec.ID, EvTimestamp: time.Now()}}
				} else {
					if s.State.Running != nil {
						c.eventQueue <- &PodEvent{common.BaseEvent{EvName: PodEngineRunningEvent, InsID: c.InsSpec.ID, EvTimestamp: time.Now()}}
					}
				}
			}
		}
	}
}

func (c *PodCollector) CollectPodNetwork() {

	command := []string{"bash", "-c", "ip a show bond1"}

	stdout, stderr, err := resource.ExecInPod(command, "manager", c.InsSpec.PodName, resource.GetClusterManagerConfig().Cluster.Namespace, 0)
	if err != nil {
		log.Warnf("Failed to collect %s bond1 network err %s", c.InsSpec.String(), err.Error())
		c.eventQueue <- &PodNetworkEvent{common.BaseEvent{EvName: PodUnknownEvent, InsID: c.InsSpec.ID, EvTimestamp: time.Now()}}
	} else {
		if strings.Contains(stdout, "NO-CARRIER") {
			c.eventQueue <- &PodNetworkEvent{common.BaseEvent{EvName: PodNetworkBond1DownEvent, InsID: c.InsSpec.ID, EvTimestamp: time.Now()}}
		} else if strings.Contains(stdout, "BROADCAST") {
			c.eventQueue <- &PodNetworkEvent{common.BaseEvent{EvName: PodNetworkBond1UpEvent, InsID: c.InsSpec.ID, EvTimestamp: time.Now()}}
		} else {
			log.Warnf("Failed to collect %s bond1 network return %s/%s", c.InsSpec.String(), stdout, stderr)
			c.eventQueue <- &PodNetworkEvent{common.BaseEvent{EvName: PodUnknownEvent, InsID: c.InsSpec.ID, EvTimestamp: time.Now()}}
		}
	}
}

func (c *PodCollector) Collect() error {

	for {
		select {
		case <-time.After(time.Millisecond * time.Duration(*common.EngineMetricsIntervalMs)):
			break
		case <-c.StopCollect:
			log.Infof("Engine metrics collector %s stop!", c.InsSpec.String())
			return nil
		}

		if resource.IsPolarBoxMode() {
			//c.CollectPodNetwork()
		} else if resource.IsCloudOperator() {
			c.CollectPodRestart()
		}
	}

	return errors.Errorf("Detector %s quit unexpected!", c.ID())
}

func (c *PodCollector) RegisterEventCallback(callback common.EventHandler) error {
	c.EventCallbacks = append(c.EventCallbacks, callback)
	return nil
}
