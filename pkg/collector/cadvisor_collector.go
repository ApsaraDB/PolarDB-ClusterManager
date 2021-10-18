package collector

import (
	"github.com/google/cadvisor/client"

	"github.com/google/cadvisor/info/v1"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"strings"
	"time"
)

const (
	NumStats           = 2
	ThrottledThreshold = 0.8
	UsageThreshold     = 0.9
	EmptyPercentage    = 0.02

	ContainerExceptionEvent string = "ContainerExceptionEvent"
	ContainerNormalEvent    string = "ContainerNormalEvent"
	ContainerUnknownEvent   string = "ContainerUnknownEvent"
	ContainerTimeoutEvent   string = "ContainerTimeoutEvent"

	ContainerCpuFullEvent   string = "ContainerCpuFullEvent"
	ContainerCpuEmptyEvent  string = "ContainerCpuEmptyEvent"
	ContainerCpuNormalEvent string = "ContainerCpuNormalEvent"

	StorageUsageFullEvent   string = "StorageUsageFullEvent"
	StorageUsageNormalEvent string = "StorageUsageNormalEvent"

	EngineMetricsUnknownEvent    = "EngineMetricsUnknownEvent"
	EngineMetricsConnFullEvent   = "EngineMetricsConnFullEvent"
	EngineMetricsConnNormalEvent = "EngineMetricsConnNormalEvent"

	EngineMetricsReplicaDelay  = "EngineMetricsReplicaDelay"
	EngineMetricsReplicaNormal = "EngineMetricsReplicaNormal"

	EngineMetricsCollectEvent = "EngineMetricsCollectEvent"
	PodMetricsCollectEvent    = "PodMetricsCollectEvent"

	EngineMetricsWaitEvent   = "EngineMetricsWaitEvent"
	EngineMetricsNoWaitEvent = "EngineMetricsNoWaitEvent"

	EngineConfCollectEvent = "EngineConfCollectEvent"

	CAdvisorCollectorEventProducer = "CAdvisorCollectorEventProducer"

	CAdvisorCollectInterval   = 3 * time.Second
	HardwareCollectorInterval = time.Second
)

type PodCpuEvent struct {
	common.BaseEvent
}

func (e *PodCpuEvent) EventCategory() common.EventCategory {
	return common.PodCpuEventCategory
}

type CAdvisorCollector struct {
	InsSpec        common.InsSpec
	ContainerName  string
	StopFlag       chan bool
	StopCollect    chan bool
	Started        bool
	EventCallbacks []common.EventHandler
	clusterID      string
	eventQueue     chan common.Event
	client         *client.Client
}

func NewCAdvisorCollector(clusterID string, insSpec *common.InsSpec) *CAdvisorCollector {
	log.Infof("CAdvisor Starting collect %s", insSpec.String())
	return &CAdvisorCollector{
		clusterID:   clusterID,
		InsSpec:     *insSpec,
		eventQueue:  make(chan common.Event, 30),
		StopFlag:    make(chan bool),
		StopCollect: make(chan bool),
		Started:     false,
	}
}

func (c *CAdvisorCollector) ID() string {
	return "cadvisorCollector@" + c.InsSpec.String()
}

func (c *CAdvisorCollector) Type() string {
	return CAdvisorCollectorEventProducer
}

func (c *CAdvisorCollector) ClusterID() string {
	return c.clusterID
}

func (c *CAdvisorCollector) EndPoint() common.EndPoint {
	return c.InsSpec.Endpoint
}

func (c *CAdvisorCollector) Start() error {
	if c.Started {
		return nil
	}
	c.Connect()
	go c.Collect()

	go func() {
		defer log.Infof("CAdvisorCollector %s quit.", c.InsSpec.String())
		for {
			select {
			case ev := <-c.eventQueue:
				for _, callback := range c.EventCallbacks {
					err := callback.HandleEvent(ev)
					if err != nil {
						log.Errorf(" CAdvisorCollector exec %v callBack event: %v , err: %v", callback.Name(), common.EnvSimpleName(ev), err)
					}
				}
			case <-c.StopFlag:
				c.StopCollect <- true
				break
			}
		}
	}()
	c.Started = true

	return nil
}

func (c *CAdvisorCollector) Stop() error {
	c.StopFlag <- true
	return nil
}

func (c *CAdvisorCollector) ReStart() error {
	return nil
}

func (c *CAdvisorCollector) Collect() error {
	if c.client == nil {
		return errors.New("CAdvisorCollector need Connect!")
	}

	for {
		select {
		case <-time.After(CAdvisorCollectInterval):
			break
		case <-c.StopCollect:
			log.Infof("CAdvisorCollector %s stop!", c.InsSpec.String())
			return nil
		}
		startTs := time.Now()
		containerInfos, err := c.CollectSubContainer(c.InsSpec.PodID)
		if err != nil {
			log.Errorf("Failed to CollectContainer %s with %v", c.InsSpec.String(), err)
			c.eventQueue <- &PodCpuEvent{
				common.BaseEvent{
					EvName:      ContainerUnknownEvent,
					EvTimestamp: time.Now(),
					InsID:       c.InsSpec.ID,
				}}
			continue
		}
		if time.Since(startTs) > time.Second {
			log.Info("Collect long time", time.Since(startTs))
		}

		var info *v1.ContainerInfo = nil
		for i, containerInfo := range containerInfos {
			for _, alias := range containerInfo.Aliases {
				if strings.Contains(alias, "k8s_engine") {
					info = &containerInfos[i]
					break
				}
			}
		}
		if info == nil || len(info.Stats) != NumStats {
			c.eventQueue <- &PodCpuEvent{
				common.BaseEvent{
					EvName:      ContainerUnknownEvent,
					EvTimestamp: time.Now(),
					InsID:       c.InsSpec.ID,
				}}
			continue
		}

		period := info.Stats[1].Cpu.CFS.Periods - info.Stats[0].Cpu.CFS.Periods
		throttled := info.Stats[1].Cpu.CFS.ThrottledPeriods - info.Stats[0].Cpu.CFS.ThrottledPeriods
		usage := float64(info.Stats[1].Cpu.Usage.Total-info.Stats[0].Cpu.Usage.Total) / float64(period*info.Spec.Cpu.Quota*1000)

		if period == 0 {
			continue
		}

		if float64(throttled)/float64(period) > ThrottledThreshold || usage > UsageThreshold {
			c.eventQueue <- &PodCpuEvent{
				common.BaseEvent{
					EvName:      ContainerCpuFullEvent,
					InsID:       c.InsSpec.ID,
					EvTimestamp: info.Stats[1].Timestamp.Local(),
				}}
		} else if throttled == 0 && usage < EmptyPercentage {
			c.eventQueue <- &PodCpuEvent{
				common.BaseEvent{
					EvName:      ContainerCpuEmptyEvent,
					InsID:       c.InsSpec.ID,
					EvTimestamp: info.Stats[1].Timestamp.Local(),
				}}
		} else {
			c.eventQueue <- &PodCpuEvent{
				common.BaseEvent{
					EvName:      ContainerCpuNormalEvent,
					InsID:       c.InsSpec.ID,
					EvTimestamp: info.Stats[1].Timestamp.Local(),
				}}
		}
	}

	return nil
}

func (c *CAdvisorCollector) Connect() (err error) {
	c.client, err = client.NewClient("http://" + c.InsSpec.Endpoint.Host + ":8001" + "/")
	return
}

func (c *CAdvisorCollector) CollectSubContainer(podID string) ([]v1.ContainerInfo, error) {
	req := &v1.ContainerInfoRequest{
		NumStats: NumStats,
	}
	containerName := "/kubepods/burstable/pod" + podID
	return c.client.SubcontainersInfo(containerName, req)
}

func (c *CAdvisorCollector) CollectContainer(podID string) (*v1.ContainerInfo, error) {
	req := &v1.ContainerInfoRequest{
		NumStats: NumStats,
	}
	containerName := "/kubepods/burstable/pod" + podID
	return c.client.ContainerInfo(containerName, req)
}

func (c *CAdvisorCollector) RegisterEventCallback(callback common.EventHandler) error {
	c.EventCallbacks = append(c.EventCallbacks, callback)
	return nil
}
