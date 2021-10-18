package collector

import (
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"time"
)

const MonitorNamePrefix = "Monitor_"

type SQLMonitorCollector struct {
	InsSpec        common.InsSpec
	StopFlag       chan bool
	ReStartFlag    chan bool
	StopCollect    chan bool
	Started        bool
	EventCallbacks []common.EventHandler
	EventQueue     chan common.Event
	EngineType     common.EngineType
	StartAt        time.Time

	copyBufferUsage   int
	sharedBufferUsage int
	recoverySize      int
}

type SqlMonitorEvent struct {
	common.BaseEvent
	Category common.EventCategory
}

func (e *SqlMonitorEvent) EventCategory() common.EventCategory {
	return e.Category
}

func NewSQLMonitorCollector(insSpec *common.InsSpec, engineType common.EngineType, rwSpec *common.InsSpec) *SQLMonitorCollector {
	log.Info("Starting SQLMonitorCollector collect", insSpec.String(), engineType, rwSpec.String())
	return &SQLMonitorCollector{
		InsSpec:     *insSpec,
		EventQueue:  make(chan common.Event, 32),
		StopFlag:    make(chan bool, 1),
		ReStartFlag: make(chan bool, 1),
		StopCollect: make(chan bool),
		Started:     false,
		EngineType:  engineType,
	}
}

func (c *SQLMonitorCollector) ID() string {
	return c.InsSpec.ID
}

func (c *SQLMonitorCollector) ClusterID() string {
	return c.InsSpec.ClusterID
}

func (c *SQLMonitorCollector) EndPoint() common.EndPoint {
	return c.InsSpec.Endpoint
}

func (c *SQLMonitorCollector) Start() error {
	if c.Started {
		return nil
	}
	c.StartAt = time.Now()
	go c.Collect()

	go func() {
		defer log.Infof("SQLMonitorCollector %s event loop quit.", c.InsSpec.String())
		for {
			select {
			case ev := <-c.EventQueue:
				if ev.Timestamp().After(c.StartAt) {
					for _, callback := range c.EventCallbacks {
						err := callback.HandleEvent(ev)
						if err != nil {
							log.Errorf(" SQLMonitorCollector exec %v callBack event: %v err: %v", callback.Name(), common.EnvSimpleName(ev), err)
						}
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

func (c *SQLMonitorCollector) ReStart() error {
	c.ReStartFlag <- true
	return nil
}

func (c *SQLMonitorCollector) Stop() error {
	c.StopFlag <- true
	return nil
}

func (c *SQLMonitorCollector) CollectSqlMonitor() error {
	config := resource.GetConfigManager().GetSQLMonitorConfig()

	type MonitorInfo struct {
		Alarm     int
		Message   string
		tableName struct{} `pg:",discard_unknown_columns"`
	}

	db := resource.GetResourceManager().GetMetricsConn(c.InsSpec.Endpoint)

	values := map[string]interface{}{}

	for _, info := range config.Items {
		if info.EngineType != c.EngineType {
			continue
		}
		var m MonitorInfo
		_, err := db.QueryOne(&m, common.InternalMarkSQL+info.CollectSQL)
		if err != nil {
			log.Warn("Failed to query", db, common.InternalMarkSQL+info.CollectSQL, err)
			continue
		}
		if m.Alarm != 0 {
			info.IsAlarm = true
			info.Msg = m.Message
		} else {
			info.IsAlarm = false
		}
		values[MonitorNamePrefix+info.Name] = info
	}

	if len(values) != 0 {
		c.EventQueue <- &SqlMonitorEvent{
			BaseEvent: common.BaseEvent{
				EvName:      EngineMetricsCollectEvent,
				EvTimestamp: time.Now(),
				InsID:       c.InsSpec.ID,
				Values:      values,
			},
			Category: common.EngineMetricsEventCategory,
		}
	}

	return nil
}

func (c *SQLMonitorCollector) Collect() error {
	defer log.Infof("SQLMonitorCollector %s collect loop quit!", c.InsSpec.String())

	for {
		select {
		case <-time.After(time.Duration(*common.EngineMetricsIntervalMs) * time.Millisecond):
			break
		case <-c.StopCollect:
			return nil
		}

		c.CollectSqlMonitor()
	}
}

func (c *SQLMonitorCollector) RegisterEventCallback(callback common.EventHandler) error {
	c.EventCallbacks = append(c.EventCallbacks, callback)
	return nil
}
