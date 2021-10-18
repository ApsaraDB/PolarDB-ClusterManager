package collector

import (
	"fmt"
	"github.com/go-pg/pg"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultCollectTimeoutSecond = 1

	EngineMetricsCollectorEventProducer = "EngineMetrcisCollectorEventProducer"
)

type EngineMetricsCollector struct {
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
	WaitEventList  []string

	copyBufferUsage      int
	sharedBufferUsage    int
	recoverySize         int
	roReplayBgReplayDiff int
	writeDelayTime       int
	delayTime            int
	delayData            int
	commitIndex          int64
	paxosRole            int
	paxosCollectFailCnt  int

	lastReplayLsn     string
	lastReplayTs      string
	standbyReplayRate float64
	standbyReplayData int

	Online      bool
	PersistSlot bool
}

type EngineMetricsEvent struct {
	common.BaseEvent
	Category common.EventCategory
}

func (e *EngineMetricsEvent) EventCategory() common.EventCategory {
	return e.Category
}

func NewEngineMetricsCollector(clusterID string, insSpec *common.InsSpec, engineType common.EngineType, rwSpec *common.InsSpec) *EngineMetricsCollector {
	log.Info("Starting collect", insSpec.String(), engineType, rwSpec.String())
	return &EngineMetricsCollector{
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

func (c *EngineMetricsCollector) ID() string {
	return c.InsSpec.ID
}

func (c *EngineMetricsCollector) Type() string {
	return EngineMetricsCollectorEventProducer
}

func (c *EngineMetricsCollector) ClusterID() string {
	return c.clusterID
}

func (c *EngineMetricsCollector) EndPoint() common.EndPoint {
	return c.InsSpec.Endpoint
}

func (c *EngineMetricsCollector) Start() error {
	if c.Started {
		return nil
	}
	c.StartAt = time.Now()
	go c.Collect()

	go func() {
		defer log.Infof("EngineMetricsCollector %s event loop quit.", c.InsSpec.String())
		for {
			select {
			case ev := <-c.EventQueue:
				if ev.Timestamp().After(c.StartAt) {
					for _, callback := range c.EventCallbacks {
						err := callback.HandleEvent(ev)
						if err != nil {
							log.Errorf(" EngineMetricsCollector exec %v callBack event: %v err: %v", callback.Name(), common.EnvSimpleName(ev), err)
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

func (c *EngineMetricsCollector) ReStart() error {
	c.ReStartFlag <- true
	return nil
}

func (c *EngineMetricsCollector) Stop() error {
	c.StopFlag <- true
	return nil
}

func (c *EngineMetricsCollector) TerminateWaitQuery() error {
	db := resource.GetResourceManager().GetMetricsConn(c.InsSpec.Endpoint)
	type WaitEvents struct {
		WaitEventType string
		WaitEvent     string
		tableName     struct{} `pg:",discard_unknown_columns"`
	}
	var events []WaitEvents

	startTs := time.Now()
	_, err := db.Query(&events, common.TerminateWaitQuerySQL)
	if err != nil {
		log.Warn("Failed to exec ", common.TerminateWaitQuerySQL, db, err, time.Since(startTs))
		return nil
	}
	for _, e := range events {
		switch e.WaitEventType {
		case "Client":
		case "Activity":
		case "":
		default:
			log.Infof("Terminate backend %v wait for %s/%s", db, e.WaitEventType, e.WaitEvent)
		}
	}

	return nil
}

func (c *EngineMetricsCollector) CollectPaxosStatus() error {
	db := resource.GetResourceManager().GetMetricsConn(c.InsSpec.Endpoint)
	type PaxosStatus struct {
		CommitIndex int64
		PaxosRole   int
		tableName   struct{} `pg:",discard_unknown_columns"`
	}
	var s PaxosStatus

	startTs := time.Now()
	_, err := db.QueryOne(&s, common.PaxosStatusSQL)
	if err != nil {
		log.Warn("Failed to exec ", common.PaxosStatusSQL, db, err, time.Since(startTs))
		c.paxosCollectFailCnt++
		if c.paxosCollectFailCnt > 5 {
			c.paxosRole = 0
			c.commitIndex = 0
		}
		return nil
	}

	c.paxosCollectFailCnt = 0
	c.commitIndex = s.CommitIndex
	c.paxosRole = s.PaxosRole

	return nil
}

func (c *EngineMetricsCollector) CollectWaitEvent() error {
	db := resource.GetResourceManager().GetMetricsConn(c.InsSpec.Endpoint)
	type WaitEvents struct {
		WaitEventType string
		WaitEvent     string
		tableName     struct{} `pg:",discard_unknown_columns"`
	}
	var events []WaitEvents

	startTs := time.Now()
	_, err := db.Query(&events, common.WaitEventSQL)
	if err != nil {
		c.WaitEventList = c.WaitEventList[0:0]
		log.Warn("Failed to collect", db, err, time.Since(startTs))
		c.EventQueue <- &EngineMetricsEvent{
			BaseEvent: common.BaseEvent{
				EvName:      EngineMetricsUnknownEvent,
				EvTimestamp: startTs,
				InsID:       c.InsSpec.ID,
			},
			Category: common.EngineWaitEventCategory,
		}
		return nil
	}
	for _, e := range events {
		switch e.WaitEventType {
		case "Client":
		case "Activity":
		case "":
		default:
			log.Debugf("%v wait for %s/%s", db, e.WaitEventType, e.WaitEvent)
			c.WaitEventList = append(c.WaitEventList, e.WaitEvent)
			if len(c.WaitEventList) > 20 {
				c.WaitEventList = c.WaitEventList[1:]
			}
			var eventList []string
			for _, ev := range c.WaitEventList {
				eventList = append(eventList, ev)
			}
			c.EventQueue <- &EngineMetricsEvent{
				BaseEvent: common.BaseEvent{
					EvName:      EngineMetricsWaitEvent,
					EvTimestamp: startTs,
					InsID:       c.InsSpec.ID,
					Values: map[string]interface{}{
						common.EventWaitEventType: e.WaitEventType,
						common.EventWaitEvent:     e.WaitEvent,
						"WaitEventList":           eventList,
					},
				},
				Category: common.EngineWaitEventCategory,
			}
			return nil
		}
	}

	c.WaitEventList = c.WaitEventList[0:0]
	c.EventQueue <- &EngineMetricsEvent{
		BaseEvent: common.BaseEvent{
			EvName:      EngineMetricsNoWaitEvent,
			EvTimestamp: startTs,
			InsID:       c.InsSpec.ID,
		},
		Category: common.EngineWaitEventCategory,
	}
	return nil

}

func (c *EngineMetricsCollector) CollectConnFull() error {
	db := resource.GetResourceManager().GetMetricsConn(c.InsSpec.Endpoint)
	var connRate float64 = 0

	startTs := time.Now()
	_, err := db.Query(pg.Scan(&connRate), common.ConnFullSQL)
	if err != nil {
		log.Warn("Failed to collect", db, err, time.Since(startTs))
		c.EventQueue <- &EngineMetricsEvent{
			BaseEvent: common.BaseEvent{
				EvName:      EngineMetricsUnknownEvent,
				EvTimestamp: startTs,
				InsID:       c.InsSpec.ID,
			},
			Category: common.EngineConnEventCategory,
		}
	} else {
		if connRate > 0.9 {
			c.EventQueue <- &EngineMetricsEvent{
				BaseEvent: common.BaseEvent{
					EvName:      EngineMetricsConnFullEvent,
					EvTimestamp: startTs,
					InsID:       c.InsSpec.ID,
				},
				Category: common.EngineConnEventCategory,
			}

			rEvent := notify.BuildDBEventPrefixByInsV2(&c.InsSpec, common.RoEngine, c.InsSpec.Endpoint)
			rEvent.Body.Describe = fmt.Sprintf("IInstance %v connection full po: %v", c.InsSpec.CustID, c.InsSpec.PodName)
			rEvent.Level = notify.EventLevel_ERROR
			rEvent.EventCode = notify.EventCode_InstanceConnectionFull
			sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
			if sErr != nil {
				log.Errorf("V2 connection full event err: %v", sErr)
			}
		} else {
			c.EventQueue <- &EngineMetricsEvent{
				BaseEvent: common.BaseEvent{
					EvName:      EngineMetricsConnNormalEvent,
					EvTimestamp: startTs,
					InsID:       c.InsSpec.ID,
				},
				Category: common.EngineConnEventCategory,
			}
		}
	}
	return nil
}

func (c *EngineMetricsCollector) CollectReplicaDelay(db *pg.DB) error {
	type ReplicationLag struct {
		ApplicationName string
		WriteLag        time.Time
		WriteLagSize    int
		ReplayLag       time.Time
		ReplayLagSize   int
	}
	var replications []ReplicationLag

	startTs := time.Now()
	_, err := db.Query(&replications, common.ReplicaDelayTimeSQL)
	if err != nil {
		log.Warn("Failed to collect", db, err, time.Since(startTs))
		return err
	}
	hasReplication := false
	for _, r := range replications {
		if resource.IsApplicationName(&c.InsSpec, r.ApplicationName) {
			replayLag := time.Duration((r.ReplayLag.Hour()*60+r.ReplayLag.Minute())*60+r.ReplayLag.Second()) * time.Second
			writeLag := time.Duration((r.WriteLag.Hour()*60+r.WriteLag.Minute())*60+r.WriteLag.Second()) * time.Second
			if c.EngineType == common.RoEngine &&
				replayLag > time.Duration(*common.EngineReplicaDelayMs)*time.Millisecond {

				c.EventQueue <- &EngineMetricsEvent{
					BaseEvent: common.BaseEvent{
						EvName:      EngineMetricsReplicaDelay,
						EvTimestamp: time.Now(),
						InsID:       c.InsSpec.ID,
						Values: map[string]interface{}{
							common.EventWriteDelayTime:  int(writeLag.Seconds()),
							common.EventWriteDelaySize:  r.WriteLagSize,
							common.EventReplayDelayTime: int(replayLag.Seconds()),
							common.EventReplayDelaySize: r.ReplayLagSize,
						},
					},
					Category: common.EngineDelayEventCategory,
				}
				log.Infof("Slot: %s Event: %s Lag:%s Time:%d Size:%d", r.ApplicationName,
					EngineMetricsReplicaDelay, r.ReplayLag.String(), int(replayLag.Seconds()), r.ReplayLagSize)
			} else {
				c.EventQueue <- &EngineMetricsEvent{
					BaseEvent: common.BaseEvent{
						EvName:      EngineMetricsReplicaNormal,
						EvTimestamp: time.Now(),
						InsID:       c.InsSpec.ID,
						Values: map[string]interface{}{
							common.EventWriteDelayTime:  int(writeLag.Seconds()),
							common.EventWriteDelaySize:  r.WriteLagSize,
							common.EventReplayDelayTime: int(replayLag.Seconds()),
							common.EventReplayDelaySize: r.ReplayLagSize,
						},
					},
					Category: common.EngineDelayEventCategory,
				}
			}
			c.delayTime = int(replayLag.Seconds())
			c.delayData = r.ReplayLagSize
			c.writeDelayTime = int(writeLag.Seconds())
			hasReplication = true
		}
	}
	if !hasReplication {
		c.EventQueue <- &EngineMetricsEvent{
			BaseEvent: common.BaseEvent{
				EvName:      EngineMetricsReplicaDelay,
				EvTimestamp: time.Now(),
				InsID:       c.InsSpec.ID,
				Values: map[string]interface{}{
					common.EventWriteDelayTime:  common.MaxReplicaDelayTime,
					common.EventWriteDelaySize:  common.MaxReplicaDelaySize,
					common.EventReplayDelayTime: common.MaxReplicaDelayTime,
					common.EventReplayDelaySize: common.MaxReplicaDelaySize,
				},
			},
			Category: common.EngineDelayEventCategory,
		}
		c.delayTime = common.MaxReplicaDelayTime
		c.delayData = common.MaxReplicaDelaySize
		log.Warnf("Failed to Collect %s replay delay from %s, empty replications", c.InsSpec.String(), c.RwSpec.String())
	}
	return nil
}

func (c *EngineMetricsCollector) CollectRoReplayBgReplayDiffSize(db *pg.DB) error {
	type DiffSize struct {
		Size int
	}
	var diff DiffSize
	_, err := db.QueryOne(&diff, common.RoReplayBgReplayDiffSQL)
	if err != nil {
		log.Warn("Failed to query", db, common.RoReplayBgReplayDiffSQL, err)
		return err
	}

	c.roReplayBgReplayDiff = diff.Size

	return nil
}

func (c *EngineMetricsCollector) CollectReplicaRestartDataSize(db *pg.DB) error {
	type ReplicationDelayDataSize struct {
		SlotName     string
		PgWalLsnDiff int64
	}
	var replications []ReplicationDelayDataSize

	_, err := db.Query(&replications, common.ReplicaDelayDataSizeSQL)
	if err != nil {
		log.Warn("Failed to query", db, common.ReplicaDelayDataSizeSQL, err)
		return err
	}
	hasReplicationSlot := false
	for _, r := range replications {
		if resource.IsApplicationName(&c.InsSpec, r.SlotName) {
			hasReplicationSlot = true
			c.EventQueue <- &EngineMetricsEvent{
				BaseEvent: common.BaseEvent{
					EvName:      EngineMetricsCollectEvent,
					EvTimestamp: time.Now(),
					InsID:       c.InsSpec.ID,
					Values: map[string]interface{}{
						common.EventReplicaRestartSize: r.PgWalLsnDiff,
					},
				},
				Category: common.EngineMetricsEventCategory,
			}
			return nil
		}
	}
	if !hasReplicationSlot {
		c.EventQueue <- &EngineMetricsEvent{
			BaseEvent: common.BaseEvent{
				EvName:      EngineMetricsReplicaDelay,
				EvTimestamp: time.Now(),
				InsID:       c.InsSpec.ID,
			},
			Category: common.EngineDelayEventCategory,
		}
		log.Infof("Ins: %s has no slot.", c.InsSpec.String())
	}
	return nil
}

func (c *EngineMetricsCollector) CollectEngineMetrics() {
	if c.EngineType == common.RwEngine {
		c.EventQueue <- &EngineMetricsEvent{
			BaseEvent: common.BaseEvent{
				EvName:      EngineMetricsCollectEvent,
				EvTimestamp: time.Now(),
				InsID:       c.InsSpec.ID,
				Values: map[string]interface{}{
					common.EventMaxRecoverySize:   c.recoverySize,
					common.EventCopyBufferUsage:   c.copyBufferUsage,
					common.EventSharedBufferUsage: c.sharedBufferUsage,
					common.EventCommitIndex:       c.commitIndex,
					common.EventPaxosRole:         c.paxosRole,
					common.EventOnlinePromote:     c.Online,
					common.EventPersistSlot:       c.PersistSlot,
				},
			},
			Category: common.EngineMetricsEventCategory,
		}
	} else if c.EngineType == common.RoEngine {
		c.EventQueue <- &EngineMetricsEvent{
			BaseEvent: common.BaseEvent{
				EvName:      EngineMetricsCollectEvent,
				EvTimestamp: time.Now(),
				InsID:       c.InsSpec.ID,
				Values: map[string]interface{}{
					common.EventRoReplayBgReplayDiffSize: c.roReplayBgReplayDiff,
					common.EventReplayDelayTime:          c.delayTime,
					common.EventReplayDelaySize:          c.delayData,
					common.EventOnlinePromote:            c.Online,
					common.EventPersistSlot:              c.PersistSlot,
				},
			},
			Category: common.EngineMetricsEventCategory,
		}
	} else if c.EngineType == common.StandbyEngine {
		var t int
		if c.standbyReplayRate == 0 {
			t = 1
		} else {
			if c.delayData != common.MaxReplicaDelaySize {
				t = int(float64(c.delayData)/c.standbyReplayRate) + 1
			} else {
				t = int(float64(c.standbyReplayData)/c.standbyReplayRate) + 1
			}
		}
		t += c.writeDelayTime
		c.EventQueue <- &EngineMetricsEvent{
			BaseEvent: common.BaseEvent{
				EvName:      EngineMetricsCollectEvent,
				EvTimestamp: time.Now(),
				InsID:       c.InsSpec.ID,
				Values: map[string]interface{}{
					common.EventReplayDelayTime:   c.delayTime,
					common.EventReplayDelaySize:   c.delayData,
					common.EventCommitIndex:       c.commitIndex,
					common.EventPaxosRole:         c.paxosRole,
					common.EventStandbyReplayRate: c.standbyReplayRate,
					common.EventStandbyReplayData: c.standbyReplayData,
					common.EventStandbyRTO:        t,
				},
			},
			Category: common.EngineMetricsEventCategory,
		}

	}
}

func (c *EngineMetricsCollector) CollectRecoverySize(db *pg.DB) error {
	type ReplicationDelayDataSize struct {
		PgWalLsnDiff int64
	}
	var replications []ReplicationDelayDataSize

	_, err := db.Query(&replications, common.RecoveryReplaySizeSQL)
	if err != nil {
		log.Warn("Failed to query", common.RecoveryReplaySizeSQL, err)
		return err
	}
	var maxDelay int64 = 0
	for _, r := range replications {
		if r.PgWalLsnDiff > maxDelay {
			maxDelay = r.PgWalLsnDiff
		}
	}
	if maxDelay != 0 {
		c.recoverySize = int(maxDelay)
	}

	return nil
}

func (c *EngineMetricsCollector) CollectCopyBufferUsage(db *pg.DB) error {
	type BufferInfo struct {
		PolarCbuf string
	}
	var bufferInfo []BufferInfo

	_, err := db.Query(&bufferInfo, common.CopyBufferUsageSQL)
	if err != nil {
		log.Warn("Failed to query", common.CopyBufferUsageSQL, err)
		return err
	} else if len(bufferInfo) != 1 {
		err = errors.Errorf("Invalid res %v by %s", bufferInfo, common.CopyBufferUsageSQL)
		log.Warn(err)
		return err
	}

	val := strings.Split(strings.Split(bufferInfo[0].PolarCbuf, ")")[0], ",")
	copy, _ := strconv.Atoi(val[1])
	release, _ := strconv.Atoi(val[4])
	copyBuffers := copy - release

	type BufferSetting struct {
		Setting int
	}
	var bufferSetting []BufferSetting
	if _, err := db.Query(&bufferSetting, common.CopyBufferSettingSQL); err != nil {
		log.Warn("Failed to query", common.CopyBufferSettingSQL, err)
		return err
	} else if len(bufferSetting) != 1 {
		errors.Errorf("Invalid res %v by %s", bufferSetting, common.CopyBufferSettingSQL)
		log.Warn(err)
		return err
	}

	c.copyBufferUsage = copyBuffers * 100 / bufferSetting[0].Setting

	return nil
}

func (c *EngineMetricsCollector) CollectSharedBufferUsage(db *pg.DB) error {
	type BufferInfo struct {
		PolarFlushlist string
	}
	var bufferInfo []BufferInfo

	_, err := db.Query(&bufferInfo, common.SharedBufferUsageSQL)
	if err != nil {
		log.Warn("Failed to query", common.SharedBufferUsageSQL, err)
		return err
	} else if len(bufferInfo) != 1 {
		err = errors.Errorf("Invalid res %v by %s", bufferInfo, common.SharedBufferUsageSQL)
		log.Warn(err)
		return err
	}

	val := strings.Split(strings.Split(bufferInfo[0].PolarFlushlist, "(")[1], ",")
	sharedBuffers, _ := strconv.Atoi(val[0])

	type BufferSetting struct {
		Setting int
	}
	var bufferSetting []BufferSetting
	if _, err := db.Query(&bufferSetting, common.SharedBufferSettingSQL); err != nil {
		log.Warn("Failed to query", common.SharedBufferSettingSQL, err)
		return err
	} else if len(bufferSetting) != 1 {
		err = errors.Errorf("Invalid res %v by %s", bufferSetting, common.SharedBufferSettingSQL)
		log.Warn(err)
		return err
	}

	c.sharedBufferUsage = sharedBuffers * 100 / bufferSetting[0].Setting

	return nil
}

func (c *EngineMetricsCollector) CollectStandbyReplayTime(db *pg.DB) error {
	var sql string
	if c.lastReplayLsn != "" {
		sql = `select pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()) as replay_data, pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '` +
			c.lastReplayLsn + `') /(extract(epoch from now())-` + c.lastReplayTs + `) as replay_rate, pg_last_wal_replay_lsn as last_replay_lsn,` +
			`extract(epoch from now()) as last_replay_ts`
	} else {
		sql = `select pg_last_wal_replay_lsn as last_replay_lsn, extract(epoch from now()) as last_replay_ts`
	}
	type StandbyDelayInfo struct {
		ReplayData    int
		ReplayRate    float64
		LastReplayLsn string
		LastReplayTs  string
	}
	var info StandbyDelayInfo

	_, err := db.Query(&info, sql)
	if err != nil {
		log.Warn("Failed to query", sql, err)
		return err
	}

	c.standbyReplayData = info.ReplayData
	c.standbyReplayRate = info.ReplayRate
	c.lastReplayLsn = info.LastReplayLsn
	c.lastReplayTs = info.LastReplayTs

	return nil
}

func (a *EngineMetricsCollector) CollectEngineVersion(db *pg.DB) error {
	a.Online = false
	a.PersistSlot = false

	type PgSetting struct {
		Name    string
		Setting string
	}
	settingMap := map[string]string{}
	var settings []PgSetting

	sql := "select name, setting from pg_settings where name in ('polar_release_date')"
	_, err := db.Query(&settings, sql)
	if err != nil {
		return errors.Wrapf(err, "Failed to query settings %s", sql)
	}
	for _, setting := range settings {
		settingMap[setting.Name] = setting.Setting
	}
	if *common.EnableOnlinePromote {
		if v, exist := settingMap["polar_release_date"]; exist && v >= "20201015" {
			a.Online = true
		}
	}
	type Config struct {
		PolarEnablePersistedPhysicalSlot string
	}
	var c Config
	sql = "show polar_enable_persisted_physical_slot"
	_, err = db.Query(&c, sql)
	if err != nil {
		return errors.Wrapf(err, "Failed to query settings %s", sql)
	}
	if c.PolarEnablePersistedPhysicalSlot == "true" || c.PolarEnablePersistedPhysicalSlot == "on" {
		a.PersistSlot = true
	}

	return nil
}

func (c *EngineMetricsCollector) Collect() error {
	defer log.Infof("EngineMetricsCollector %s collect loop quit!", c.InsSpec.String())

	for {
		select {
		case <-time.After(time.Duration(*common.EngineMetricsIntervalMs) * time.Millisecond):
			break
		case <-c.StopCollect:
			return nil
		}
		rwDb := resource.GetResourceManager().GetMetricsConn(c.RwSpec.Endpoint)
		db := resource.GetResourceManager().GetMetricsConn(c.InsSpec.Endpoint)

		c.CollectConnFull()
		c.CollectWaitEvent()
		c.TerminateWaitQuery()

		if c.InsSpec.ClusterType == common.PaxosCluster {
			c.CollectPaxosStatus()
		}

		if c.EngineType == common.RoEngine {
			c.CollectReplicaDelay(rwDb)
			//c.CollectRoReplayBgReplayDiffSize(db)
			c.CollectReplicaRestartDataSize(rwDb)
			c.CollectEngineVersion(db)
		} else if c.EngineType == common.RwEngine {
			if resource.IsPolarSharedMode() {
				c.CollectRecoverySize(db)
				c.CollectSharedBufferUsage(db)
				c.CollectCopyBufferUsage(db)
				c.CollectEngineVersion(db)
			}
		} else if c.EngineType == common.StandbyEngine {
			c.CollectReplicaDelay(rwDb)
			c.CollectStandbyReplayTime(db)
		}

		c.CollectEngineMetrics()
	}
}

func (c *EngineMetricsCollector) RegisterEventCallback(callback common.EventHandler) error {
	c.EventCallbacks = append(c.EventCallbacks, callback)
	return nil
}
