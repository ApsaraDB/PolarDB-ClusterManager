package detector

import (
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"strings"
	"time"
)

type PodEngineEvent struct {
	common.BaseEvent
}

func (e *PodEngineEvent) EventCategory() common.EventCategory {
	return common.PodEngineEventCategory
}

type PodEngineDetector struct {
	EventCallbacks []common.EventHandler
	detectSQL      string
	spec           common.InsSpec
	eventQueue     chan common.Event
	stop           chan bool
	stopDetect     chan bool
	Started        bool
}

func NewPodEngineDetector(spec common.InsSpec, detectSQL string) *PodEngineDetector {
	return &PodEngineDetector{
		spec:       spec,
		detectSQL:  detectSQL,
		eventQueue: make(chan common.Event, 10),
		stop:       make(chan bool, 10),
		stopDetect: make(chan bool, 1),
		Started:    false,
	}

}

func (d *PodEngineDetector) ID() string {
	return "PodEngineDetector@" + d.spec.String()
}

func (d *PodEngineDetector) Type() string {
	return common.EngineDetectorEventProducer
}

func (d *PodEngineDetector) ClusterID() string {
	return d.spec.ClusterID
}

func (d *PodEngineDetector) EndPoint() common.EndPoint {
	return d.spec.VipEndpoint
}

func (d *PodEngineDetector) RegisterEventCallback(callback common.EventHandler) error {
	d.EventCallbacks = append(d.EventCallbacks, callback)
	return nil
}

func (d *PodEngineDetector) Start() error {
	if d.Started {
		return nil
	}
	log.Infof("Starting PodEngineDetector %s detect %s with %s", d.ID(), d.spec.String(), d.detectSQL)

	go d.detect()

	go func() {
		defer log.Infof("PodEngineDetector %s quit.", d.ID())
		for {
			select {
			case ev := <-d.eventQueue:
				for _, c := range d.EventCallbacks {
					err := c.HandleEvent(ev)
					if err != nil {
						log.Errorf(" PodEngineDetector exec %v callBack event: %v , err: %v", c.Name(), common.EnvSimpleName(ev), err)
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

func (d *PodEngineDetector) Stop() error {
	d.stop <- true
	return nil
}

func (d *PodEngineDetector) ReStart() error {
	return nil
}

func (d *PodEngineDetector) detect() error {
	for {
		errCh := make(chan error, 1)
		startTs := time.Now()

		go func() {
			engineManager := resource.GetResourceManager().GetEngineManager(d.spec.Endpoint)
			start := time.Now()
			stdout, stderr, err := engineManager.ExecSQL(d.detectSQL, time.Duration(*common.EngineMetricsTimeoutMs)*time.Millisecond)
			if err != nil {
				log.Warnf("Failed to Detect %s err %s cost %s", d.spec.String(), err.Error(), time.Since(start).String())
			} else if stdout != "" {
				err = errors.New(stdout)
				log.Warnf("Failed to Detect %s err %s cost %s", d.spec.String(), err.Error(), time.Since(start).String())
			} else if stderr != "" {
				err = errors.New(stderr)
				log.Warnf("Failed to Detect %s err %s cost %s", d.spec.String(), err.Error(), time.Since(start).String())
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
						d.eventQueue <- &PodEngineEvent{
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
					d.eventQueue <- &PodEngineEvent{
						BaseEvent: common.BaseEvent{
							EvName: EngineFailEvent,
							InsID:  d.spec.ID,
							Values: map[string]interface{}{
								common.EventReason: err.Error(),
							},
							EvTimestamp: time.Now()},
					}
				}

				rEvent := notify.BuildDBEventPrefixByInsV2(&d.spec, common.RwEngine, d.spec.Endpoint)
				rEvent.Body.Describe = fmt.Sprintf("Instance %v detect err: %v . po: %s, ep: %s", d.spec.CustID, err.Error(), d.spec.PodName, d.spec.Endpoint)
				rEvent.Level = notify.EventLevel_ERROR
				rEvent.EventCode = notify.EventCode_InstanceDetectError
				sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
				if sErr != nil {
					log.Errorf("detect err: %v", sErr)
				}
			} else {
				d.eventQueue <- &PodEngineEvent{
					BaseEvent: common.BaseEvent{
						EvName:      EngineAliveEvent,
						InsID:       d.spec.ID,
						EvTimestamp: time.Now()},
				}
			}
			if time.Since(startTs) < (time.Duration(*common.EngineMetricsIntervalMs) * time.Millisecond) {
				time.Sleep((time.Duration(*common.EngineMetricsIntervalMs) * time.Millisecond) - time.Since(startTs))
			}
		case <-time.After(time.Duration(*common.EngineMetricsTimeoutMs) * time.Millisecond):
			log.Warnf("Failed to Detect %s err timeout", d.spec.String())
			d.eventQueue <- &PodEngineEvent{
				BaseEvent: common.BaseEvent{
					EvName: EngineFailEvent,
					InsID:  d.spec.ID,
					Values: map[string]interface{}{
						common.EventReason: "timeout",
					},
					EvTimestamp: time.Now()},
			}

			rEvent := notify.BuildDBEventPrefixByInsV2(&d.spec, common.RwEngine, d.spec.Endpoint)
			rEvent.Body.Describe = fmt.Sprintf("Instance %v detect timeout . po: %s, ep: %s", d.spec.CustID, d.spec.PodName, d.spec.Endpoint)
			rEvent.Level = notify.EventLevel_ERROR
			rEvent.EventCode = notify.EventCode_InstanceDetectTimeOut
			sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
			if sErr != nil {
				log.Errorf("detect err: %v", sErr)
			}
		case <-d.stopDetect:
			log.Infof("PodEngineDetector %s quit.", d.spec.String())
			return nil
		}
	}

	return errors.Errorf("PodEngineDetector %s quit unexpected!", d.spec.String())
}
