package collector

import (
	"context"
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/polardb"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"google.golang.org/grpc"
	"strconv"
	"time"
)

const (
	AgentHeartbeatCollectorEventProducer = "AgentHeartbeatCollectorEventProducer"
)

type AgentHeartbeatCollector struct {
	clusterID      string
	InsSpec        common.InsSpec
	StopFlag       chan bool
	ReStartFlag    chan bool
	StopCollect    chan bool
	Started        bool
	EventCallbacks []common.EventHandler
	EventQueue     chan common.Event
	EngineType     common.EngineType
	RwSpec         common.InsSpec
	StartAt        time.Time
	Category       common.EventCategory
	AgentPort      int
	errTimes       int
	lastErr        string
}

type AgentHeartbeatCollectEvent struct {
	common.BaseEvent
	Category common.EventCategory
}

func (e *AgentHeartbeatCollectEvent) EventCategory() common.EventCategory {
	return e.Category
}

func NewAgentHeartbeatCollector(
	clusterID string, insSpec *common.InsSpec, engineType common.EngineType, rwSpec *common.InsSpec, category common.EventCategory, port int) *AgentHeartbeatCollector {
	log.Infof("Starting heartbeat ins %s type %s rw %s category %s port %d", insSpec.String(), engineType, rwSpec.String(), category, port)
	return &AgentHeartbeatCollector{
		clusterID:   clusterID,
		InsSpec:     *insSpec,
		EventQueue:  make(chan common.Event, 32),
		StopFlag:    make(chan bool, 1),
		ReStartFlag: make(chan bool, 1),
		StopCollect: make(chan bool),
		Started:     false,
		EngineType:  engineType,
		RwSpec:      *rwSpec,
		Category:    category,
		AgentPort:   port,
	}
}

func (c *AgentHeartbeatCollector) String() string {
	return fmt.Sprintf("ins %s type %s rw %s category %s port %d", c.InsSpec.String(), c.EngineType, c.RwSpec.String(), c.Category, c.AgentPort)
}

func (c *AgentHeartbeatCollector) ID() string {
	return c.InsSpec.ID
}

func (c *AgentHeartbeatCollector) Type() string {
	return AgentHeartbeatCollectorEventProducer
}

func (c *AgentHeartbeatCollector) ClusterID() string {
	return c.clusterID
}

func (c *AgentHeartbeatCollector) EndPoint() common.EndPoint {
	return c.InsSpec.Endpoint
}

func (c *AgentHeartbeatCollector) Start() error {
	if c.Started {
		return nil
	}
	c.StartAt = time.Now()
	go c.Collect()

	go func() {
		defer log.Infof("AgentHeartbeatCollector %s event loop quit.", c.String())
		for {
			select {
			case ev := <-c.EventQueue:
				if ev.Timestamp().After(c.StartAt) {
					for _, callback := range c.EventCallbacks {
						callback.HandleEvent(ev)
					}
				} else {
					log.Debugf("Skip event %s ts %s before StartAt %s", ev.Name(), ev.Timestamp().String(), c.StartAt.String())
				}
			case <-c.StopFlag:
				c.StopCollect <- true
				return
			case <-c.ReStartFlag:
				c.StopCollect <- true
				c.StartAt = time.Now()
				go c.Collect()
			}
		}
	}()
	c.Started = true

	return nil
}

func (c *AgentHeartbeatCollector) ReStart() error {
	c.ReStartFlag <- true
	return nil
}

func (c *AgentHeartbeatCollector) Stop() error {
	c.StopFlag <- true
	return nil
}

func (c *AgentHeartbeatCollector) SendEvent(err error) error {
	if err != nil {
		if err.Error() != c.lastErr {
			log.Warnf("Failed to HeartBeat %s with %v", c.String(), err.Error())
			c.errTimes = 0
		}
		if c.errTimes < 10 || c.errTimes%300 == 0 {
			log.Warnf("Failed to HeartBeat %s with %v", c.String(), err.Error())
		}
		c.lastErr = err.Error()
		c.errTimes++
		c.EventQueue <- &AgentHeartbeatCollectEvent{
			BaseEvent: common.BaseEvent{
				EvName:      EngineFailEvent,
				EvTimestamp: time.Now(),
				InsID:       c.InsSpec.ID,
				Values: map[string]interface{}{
					common.EventReason: err.Error(),
				},
			},
			Category: c.Category,
		}
	} else {
		c.errTimes = 0
		c.EventQueue <- &AgentHeartbeatCollectEvent{
			BaseEvent: common.BaseEvent{
				EvName:      EngineAliveEvent,
				EvTimestamp: time.Now(),
				InsID:       c.InsSpec.ID,
			},
			Category: c.Category,
		}
	}

	return nil
}

func (c *AgentHeartbeatCollector) CollectBackupAgent() error {

	addr := c.InsSpec.Endpoint.Host + ":" + strconv.Itoa(c.AgentPort)
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return c.SendEvent(errors.Errorf("Failed to connect agent %s err=%s", addr, err.Error()))
	}
	defer conn.Close()

	ctx, _ := context.WithTimeout(context.Background(), time.Second*60)

	port, _ := strconv.Atoi(c.InsSpec.Endpoint.Port)
	rwPort, _ := strconv.Atoi(c.RwSpec.Endpoint.Port)
	insType := "db"
	if c.EngineType == common.Proxy {
		insType = "maxscale"
	} else if c.EngineType == common.ClusterManager {
		insType = "cm"
	}

	req := &polardb.SyncInstanceRequest{
		SystemIdentify: resource.GetClusterManagerConfig().SystemIdentify,
		Ins: &polardb.InsSpec{
			Host:     c.InsSpec.Endpoint.Host,
			Port:     int32(port),
			Username: resource.GetClusterManagerConfig().Account.AuroraUser,
			Password: resource.GetClusterManagerConfig().Account.AuroraPassword,
			Database: "polardb_admin",
			Instype:  insType,
		},
		Rw: &polardb.InsSpec{
			Host:     c.RwSpec.Endpoint.Host,
			Port:     int32(rwPort),
			Username: resource.GetClusterManagerConfig().Account.AuroraUser,
			Password: resource.GetClusterManagerConfig().Account.AuroraPassword,
			Database: "polardb_admin",
			Instype:  "db",
		},
	}
	resp, err := polardb.NewControlServiceClient(conn).SyncInstance(ctx, req)
	if err != nil {
		return c.SendEvent(errors.Errorf("Failed to request agent %s err=%s", addr, err.Error()))
	}

	if resp.Code != 0 {
		return c.SendEvent(errors.Errorf("Failed to request %s errcode %d errmsg %s", addr, resp.Code, resp.Msg))
	}

	return c.SendEvent(nil)

}

func (c *AgentHeartbeatCollector) Collect() error {
	log.Infof("AgentHeartbeatCollector %s collect loop start!", c.String())
	defer log.Infof("AgentHeartbeatCollector %s collect loop quit!", c.String())

	for {
		select {
		case <-time.After(time.Duration(*common.EngineConfIntervalMs) * time.Millisecond):
			break
		case <-c.StopCollect:
			return nil
		}

		c.CollectBackupAgent()
	}
}

func (c *AgentHeartbeatCollector) RegisterEventCallback(callback common.EventHandler) error {
	c.EventCallbacks = append(c.EventCallbacks, callback)
	return nil
}
