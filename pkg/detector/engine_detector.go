package detector

import (
	"fmt"
	"github.com/go-pg/pg"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"os"
	"runtime"
	"strings"
	"time"
)

const (
	EngineAliveEvent string = "EngineAliveEvent"
	EngineFailEvent  string = "EngineFailEvent"
)

type EngineEvent struct {
	common.BaseEvent
}

func (e *EngineEvent) EventCategory() common.EventCategory {
	return common.EngineEventCategory
}

type EngineDetector struct {
	EventCallbacks    []common.EventHandler
	DetectorClusterID string
	User              string
	Password          string
	DetectSQL         string
	Spec              common.InsSpec
	EventQueue        chan common.Event
	StopFlag          chan bool
	ReStartFlag       chan bool
	StopDetect        chan bool
	Started           bool
	StartAt           time.Time
	ExcludeFailReason map[string]string
	InterfaceErrCount int
}

func NewEngineDetector(clusterID string, spec common.InsSpec, user, password, detectSQL string) *EngineDetector {
	return &EngineDetector{
		DetectorClusterID: clusterID,
		Spec:              spec,
		DetectSQL:         detectSQL,
		EventQueue:        make(chan common.Event, 32),
		StopFlag:          make(chan bool, 1),
		ReStartFlag:       make(chan bool, 1),
		StopDetect:        make(chan bool),
		User:              user,
		Password:          password,
		Started:           false,
		ExcludeFailReason: make(map[string]string),
	}

}

func (d *EngineDetector) ID() string {
	return "Detector@" + d.Spec.String()
}

func (d *EngineDetector) Type() string {
	return common.EngineDetectorEventProducer
}

func (d *EngineDetector) ClusterID() string {
	return d.DetectorClusterID
}

func (d *EngineDetector) EndPoint() common.EndPoint {
	return d.Spec.Endpoint
}

func (d *EngineDetector) RegisterEventCallback(callback common.EventHandler) error {
	d.EventCallbacks = append(d.EventCallbacks, callback)
	return nil
}

func (d *EngineDetector) Start() error {
	if d.Started {
		return nil
	}
	log.Infof("Starting EngineDetector %s detect %s with %s", d.ID(), d.Spec.String(), d.DetectSQL)

	// exclude px ro error msg
	d.ExcludeFailReason[common.EnginePXMoreSegs] = ""
	d.ExcludeFailReason[common.EngineWrapAroundDataLoss] = ""

	d.StartAt = time.Now()
	go d.detect()

	go func() {
		defer log.Infof("EngineDetector %s event loop quit.", d.ID())
		for {
			select {
			case ev := <-d.EventQueue:
				if ev.Timestamp().After(d.StartAt) {
					for _, callback := range d.EventCallbacks {
						err := callback.HandleEvent(ev)
						if err != nil {
							log.Errorf(" EngineDetector exec %v callBack event: %v , err: %v", callback.Name(), common.EnvSimpleName(ev), err)
						}
					}
				} else {
					log.Debugf("Skip event %s ts %s before StartAt %s", ev.Name(), ev.Timestamp().String(), d.StartAt.String())
				}
			case <-d.StopFlag:
				d.StopDetect <- true
				return
			case <-d.ReStartFlag:
				d.StopDetect <- true
				d.StartAt = time.Now()
				go d.detect()
			}
		}
	}()
	d.Started = true

	return nil
}

func (d *EngineDetector) ReStart() error {
	d.ReStartFlag <- true
	return nil
}

func (d *EngineDetector) Stop() error {
	d.StopFlag <- true
	return nil
}

func (d *EngineDetector) detect() error {

	_, fileName, line, ok := runtime.Caller(1)
	if !ok {
		fileName = "???"
		line = 0
	}
	short := fileName
	for i := len(fileName) - 1; i > 0; i-- {
		if fileName[i] == '/' {
			short = fileName[i+1:]
			break
		}
	}
	head := fmt.Sprintf("%v:%v", short, line)

	backupErrCh := make(chan error, 1)
	hasBackupReq := false
	var backupStartTs time.Time

	for {
		errCh := make(chan error, 1)
		var id int64

		startTs := time.Now()

		go func() {
			db := resource.GetResourceManager().GetEngineConn(d.Spec.Endpoint)
			start := time.Now()
			_, err := db.Query(pg.Scan(&id), d.DetectSQL)
			if err != nil {
				log.Warn(head, ":Failed to Detect", db, err, time.Since(start))
				resource.GetResourceManager().ResetEngineConn(d.Spec.Endpoint)
			}
			errCh <- err
		}()

		var err error
		select {
		case err = <-backupErrCh:
			if err == nil {
				d.EventQueue <- &EngineEvent{
					BaseEvent: common.BaseEvent{
						EvName:      EngineAliveEvent,
						Ep:          d.EndPoint(),
						InsID:       d.Spec.ID,
						EvTimestamp: time.Now()},
				}
			}
			hasBackupReq = false
		case err = <-errCh:
			if err != nil {
				if strings.Contains(err.Error(), common.EngineTimeout) && !hasBackupReq {
					hasBackupReq = true
					backupStartTs = time.Now()
					go func() {
						db := pg.Connect(&pg.Options{
							Addr:            d.Spec.Endpoint.String(),
							User:            resource.GetClusterManagerConfig().Account.AuroraUser,
							Password:        resource.GetClusterManagerConfig().Account.AuroraPassword,
							Database:        "polardb_admin",
							PoolSize:        1,
							DialTimeout:     time.Second * 60,
							ReadTimeout:     time.Second * 60,
							WriteTimeout:    time.Second * 60,
							ApplicationName: "ClusterManager BackupRequest",
						})
						_, err := db.Query(pg.Scan(&id), d.DetectSQL)
						if err != nil {
							log.Warn(head, ":Failed to Detect with timeout 60s ", db, err, time.Since(backupStartTs))
						} else {
							log.Infof("Success to backup request %s with %s", d.Spec.String(), time.Since(backupStartTs))
						}
						db.Close()
						backupErrCh <- err
					}()
				}

				if *common.EnablePolarStackInternalPolicy && resource.IsPolarBoxMode() {
					go func() {
						nodeName := os.Getenv("HOSTNAME")
						nodeInfo, err := resource.GetResourceManager().GetK8sClient().GetNodeInfo(nodeName)
						if err == nil {
							for _, cond := range nodeInfo.Status.Conditions {
								// 探测到cm节点用户网络连接断开(拔线场景)
								if cond.Type == "NodeClientNetworkUnavailable" && cond.Status == "True" {
									d.InterfaceErrCount++
									if d.InterfaceErrCount > 5 {
										log.Fatalf("Detect CM Node %s NodeClientNetworkUnavailable %d times exit", nodeName, d.InterfaceErrCount)
									} else {
										log.Infof("Detect CM Node %s NodeClientNetworkUnavailable %d times", nodeName, d.InterfaceErrCount)
										backupErrCh <- nil
										return
									}
								}
							}
						} else {
							log.Warnf("Failed to get node %s NodeClientNetworkUnavailable err %s", nodeName, err.Error())
							if d.InterfaceErrCount > 0 {
								log.Warnf("CM Node %s interface already down, skip detect error", nodeName)
								backupErrCh <- nil
								return
							}
						}
					}()

					if d.InterfaceErrCount > 3 {
						log.Warnf("CM Node interface already down more than %d times, skip detect error", d.InterfaceErrCount)
						break
					}
				}

				hasDetailError := false
				for _, reason := range common.DetailFailReason {
					if strings.Contains(err.Error(), reason) {
						if _, exist := d.ExcludeFailReason[reason]; exist {
							d.EventQueue <- &EngineEvent{
								BaseEvent: common.BaseEvent{
									EvName:      EngineAliveEvent,
									Ep:          d.EndPoint(),
									InsID:       d.Spec.ID,
									EvTimestamp: startTs},
							}
						} else {
							d.EventQueue <- &EngineEvent{
								BaseEvent: common.BaseEvent{
									EvName: EngineFailEvent,
									Ep:     d.EndPoint(),
									InsID:  d.Spec.ID,
									Values: map[string]interface{}{
										common.EventReason: reason,
									},
									EvTimestamp: startTs},
							}

							rEvent := notify.BuildDBEventPrefixByInsV2(&d.Spec, common.RwEngine, d.Spec.Endpoint)
							rEvent.Body.Describe = fmt.Sprintf("Instance %v detect error: %v . po: %s, ep: %s", d.Spec.CustID, err.Error(), d.Spec.PodName, d.Spec.Endpoint)
							rEvent.Level = notify.EventLevel_ERROR
							rEvent.EventCode = notify.EventCode_InstanceDetectError
							sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
							if sErr != nil {
								log.Errorf("detect err: %v", sErr)
							}
						}
						hasDetailError = true
					}
				}
				if !hasDetailError {
					d.EventQueue <- &EngineEvent{
						BaseEvent: common.BaseEvent{
							EvName: EngineFailEvent,
							Ep:     d.EndPoint(),
							InsID:  d.Spec.ID,
							Values: map[string]interface{}{
								common.EventReason: err.Error(),
							},
							EvTimestamp: startTs},
					}

					rEvent := notify.BuildDBEventPrefixByInsV2(&d.Spec, common.RwEngine, d.Spec.Endpoint)
					rEvent.Body.Describe = fmt.Sprintf("Instance %v detect error: %v . po: %s, ep: %s", d.Spec.CustID, err.Error(), d.Spec.PodName, d.Spec.Endpoint)
					rEvent.Level = notify.EventLevel_ERROR
					rEvent.EventCode = notify.EventCode_InstanceDetectTimeOut
					sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
					if sErr != nil {
						log.Errorf("detect err: %v", sErr)
					}
				}
			} else {
				d.EventQueue <- &EngineEvent{
					BaseEvent: common.BaseEvent{
						EvName:      EngineAliveEvent,
						Ep:          d.EndPoint(),
						InsID:       d.Spec.ID,
						EvTimestamp: startTs},
				}
				d.InterfaceErrCount = 0
			}

		case <-d.StopDetect:
			log.Infof("EngineDetector %s detect loop quit.", d.Spec.String())
			return nil
		}

		if time.Since(startTs) < (time.Duration(*common.EngineDetectIntervalMs) * time.Millisecond) {
			time.Sleep((time.Duration(*common.EngineDetectIntervalMs) * time.Millisecond) - time.Since(startTs))
		}

	}
}
