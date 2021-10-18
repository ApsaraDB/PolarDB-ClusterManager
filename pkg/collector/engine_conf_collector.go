package collector

import (
	"github.com/go-pg/pg"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"time"
)

const (
	EngineConfCollectorEventProducer = "EngineMetrcisCollectorEventProducer"
)

type EngineConfCollector struct {
	clusterID         string
	InsSpec           common.InsSpec
	StopFlag          chan bool
	ReStartFlag       chan bool
	StopCollect       chan bool
	Started           bool
	EventCallbacks    []common.EventHandler
	EventQueue        chan common.Event
	EngineType        common.EngineType
	RwSpec            common.InsSpec
	StartAt           time.Time
	FileConfTimestamp string
}

type EngineConfEvent struct {
	common.BaseEvent
	Category common.EventCategory
}

func (e *EngineConfEvent) EventCategory() common.EventCategory {
	return e.Category
}

func NewEngineConfCollector(clusterID string, insSpec *common.InsSpec, engineType common.EngineType, rwSpec *common.InsSpec) *EngineConfCollector {
	log.Info("Starting collect", insSpec.String(), engineType, rwSpec.String())
	return &EngineConfCollector{
		clusterID:   clusterID,
		InsSpec:     *insSpec,
		EventQueue:  make(chan common.Event, 32),
		StopFlag:    make(chan bool, 1),
		ReStartFlag: make(chan bool, 1),
		StopCollect: make(chan bool),
		Started:     false,
		EngineType:  engineType,
		RwSpec:      *rwSpec,
	}
}

func (c *EngineConfCollector) ID() string {
	return c.InsSpec.ID
}

func (c *EngineConfCollector) Type() string {
	return EngineConfCollectorEventProducer
}

func (c *EngineConfCollector) ClusterID() string {
	return c.clusterID
}

func (c *EngineConfCollector) EndPoint() common.EndPoint {
	return c.InsSpec.Endpoint
}

func (c *EngineConfCollector) Start() error {
	if c.Started {
		return nil
	}
	c.StartAt = time.Now()
	go c.Collect()

	go func() {
		defer log.Infof("EngineConfCollector %s event loop quit.", c.InsSpec.String())
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

func (c *EngineConfCollector) ReStart() error {
	c.ReStartFlag <- true
	return nil
}

func (c *EngineConfCollector) Stop() error {
	c.StopFlag <- true
	return nil
}

func (c *EngineConfCollector) CollectFileAppliedConf(db *pg.DB) error {
	startTs := time.Now()

	/*
		var fileConfTimestamp string
		_, err := db.QueryOne(&fileConfTimestamp, common.CollectFileConfTimestampSQL)
		if err != nil {
			log.Warn("Failed to collect", db, common.CollectFileConfTimestampSQL, err, time.Since(startTs))
			return nil
		}

		if fileConfTimestamp == c.FileConfTimestamp {
			var errorFileConf string
			_, err := db.QueryOne(&errorFileConf, common.CollectErrorFileConfSQL)
			if err != nil {
				log.Warn("Failed to collect", db, common.CollectErrorFileConfSQL, err, time.Since(startTs))
				return nil
			}
		}
		c.FileConfTimestamp = fileConfTimestamp
		log.Infof("ins %s file conf reload timestamp %s", c.InsSpec.String(), c.FileConfTimestamp)
	*/

	var fileSettings []common.FileSetting

	startTs = time.Now()
	_, err := db.Query(&fileSettings, common.CollectFileConfSQL)
	if err != nil {
		log.Warn("Failed to collect", db, common.CollectFileConfSQL, err, time.Since(startTs))
		return nil
	}

	c.EventQueue <- &EngineConfEvent{
		BaseEvent: common.BaseEvent{
			EvName:      EngineConfCollectEvent,
			EvTimestamp: startTs,
			InsID:       c.InsSpec.ID,
			Values: map[string]interface{}{
				common.EventFileConf: fileSettings,
			},
		},
		Category: common.EngineConfEventCategory,
	}
	return nil

}

func (c *EngineConfCollector) Collect() error {
	log.Infof("EngineConfCollector %s collect loop start!", c.InsSpec.String())
	defer log.Infof("EngineConfCollector %s collect loop quit!", c.InsSpec.String())

	for {
		select {
		case <-time.After(time.Duration(*common.EngineConfIntervalMs) * time.Millisecond):
			break
		case <-c.StopCollect:
			return nil
		}

		db := resource.GetResourceManager().GetMetricsConn(c.InsSpec.Endpoint)

		c.CollectFileAppliedConf(db)
	}
}

func (c *EngineConfCollector) RegisterEventCallback(callback common.EventHandler) error {
	c.EventCallbacks = append(c.EventCallbacks, callback)
	return nil
}
