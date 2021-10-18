package detector

import (
	"github.com/go-pg/pg"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"strings"
	"time"
)

type VipEvent struct {
	common.BaseEvent
}

func (e *VipEvent) EventCategory() common.EventCategory {
	return common.VipEventCategory
}

type VipDetector struct {
	EventCallbacks []common.EventHandler
	clusterID      string
	user           string
	password       string
	detectSQL      string
	spec           common.InsSpec
	eventQueue     chan common.Event
	stop           chan bool
	stopDetect     chan bool
	Started        bool
}

func NewVipDetector(clusterID string, spec common.InsSpec, user, password, detectSQL string) *VipDetector {
	return &VipDetector{
		clusterID:  clusterID,
		spec:       spec,
		detectSQL:  detectSQL,
		eventQueue: make(chan common.Event, 10),
		stop:       make(chan bool, 10),
		stopDetect: make(chan bool, 1),
		user:       user,
		password:   password,
		Started:    false,
	}

}

func (d *VipDetector) ID() string {
	return "Detector@" + d.spec.String()
}

func (d *VipDetector) Type() string {
	return common.EngineDetectorEventProducer
}

func (d *VipDetector) ClusterID() string {
	return d.clusterID
}

func (d *VipDetector) EndPoint() common.EndPoint {
	return d.spec.VipEndpoint
}

func (d *VipDetector) RegisterEventCallback(callback common.EventHandler) error {
	d.EventCallbacks = append(d.EventCallbacks, callback)
	return nil
}

func (d *VipDetector) Start() error {
	if d.Started {
		return nil
	}
	log.Infof("Starting VipDetector %s detect %s with %s", d.ID(), d.spec.String(), d.detectSQL)

	go d.detect()

	go func() {
		defer log.Infof("VipDetector %s quit.", d.ID())
		for {
			select {
			case ev := <-d.eventQueue:
				for _, c := range d.EventCallbacks {
					err := c.HandleEvent(ev)
					if err != nil {
						log.Errorf(" VipDetector exec %v callBack event: %v , err: %v", c.Name(), common.EnvSimpleName(ev), err)
					}
				}
			case <-d.stop:
				d.stopDetect <- true
				return
			}
		}
	}()
	d.Started = true

	return nil
}

func (d *VipDetector) Stop() error {
	d.stop <- true
	return nil
}

func (d *VipDetector) ReStart() error {
	return nil
}

func (d *VipDetector) detect() error {
	for {
		errCh := make(chan error, 1)
		var id int64

		startTs := time.Now()

		go func() {
			db := resource.GetResourceManager().GetEngineConn(d.spec.VipEndpoint)
			start := time.Now()
			_, err := db.Query(pg.Scan(&id), d.detectSQL)
			if err != nil {
				log.Warn("Failed to Detect", db, err, time.Since(start))
				resource.GetResourceManager().ResetEngineConn(d.spec.VipEndpoint)
			}
			errCh <- err
		}()

		var err error
		select {
		case err = <-errCh:
			if err != nil {
				hasDetailError := false
				for _, reason := range common.DetailFailReason {
					if strings.Contains(err.Error(), reason) {
						d.eventQueue <- &VipEvent{
							BaseEvent: common.BaseEvent{
								EvName: EngineFailEvent,
								InsID:  d.spec.ID,
								Values: map[string]interface{}{
									common.EventReason: reason,
								},
								EvTimestamp: time.Now()},
						}
						hasDetailError = true
					}
				}
				if !hasDetailError {
					d.eventQueue <- &VipEvent{
						BaseEvent: common.BaseEvent{
							EvName: EngineFailEvent,
							InsID:  d.spec.ID,
							Values: map[string]interface{}{
								common.EventReason: err.Error(),
							},
							EvTimestamp: time.Now()},
					}
				}
			} else {
				d.eventQueue <- &VipEvent{
					BaseEvent: common.BaseEvent{
						EvName:      EngineAliveEvent,
						InsID:       d.spec.ID,
						EvTimestamp: time.Now()},
				}
			}
			if time.Since(startTs) < (time.Duration(*common.EngineDetectIntervalMs) * time.Millisecond) {
				time.Sleep((time.Duration(*common.EngineDetectIntervalMs) * time.Millisecond) - time.Since(startTs))
			}
		case <-d.stopDetect:
			log.Infof("Detector %s quit.", d.spec.String())
			return nil
		}
	}

	return errors.Errorf("Detector %s quit unexpected!", d.spec.String())
}
