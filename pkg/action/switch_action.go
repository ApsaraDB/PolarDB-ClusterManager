package action

import (
	"fmt"
	"github.com/go-pg/pg"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"k8s.io/apimachinery/pkg/util/uuid"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SwitchActionExecutor struct {
	SwitchAction common.SwitchAction
	Event        common.SwitchEvent
	Online       bool
	Rto          time.Duration
	PersistSlot  bool
}

type SimpleFunc func() error

func ExecuteWithTimeoutRetry(f SimpleFunc, times uint32, timeout time.Duration, suicide bool) error {
	t := times
	for {
		err := f()
		if err != nil {
			if times > 0 {
				log.Warnf("Failed to execute %v ret %s, retrying", f, err.Error())
				if timeout > 0 {
					time.Sleep(timeout)
				}
			} else {
				log.Warnf("Failed to execute %v ret %s, retried %d times", f, err.Error(), t)
				if suicide {
					log.Errorf("ExecuteWithTimeoutRetry raise panic err: %v", err)

					funPc, fileName, line, ok := runtime.Caller(1)
					callFunName := runtime.FuncForPC(funPc).Name()
					if strings.HasSuffix(callFunName, "action.ExecuteWithRetry") {
						//如果上级call是 ExecuteWithRetry 方法
						funPc, fileName, line, ok = runtime.Caller(2)
					}

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

					callFunName = runtime.FuncForPC(funPc).Name()
					callFunNameShort := callFunName
					for i := len(callFunName) - 1; i > 0; i-- {
						if callFunName[i] == '/' {
							callFunNameShort = callFunName[i+1:]
							break
						}
					}

					simpleFunName := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
					simpleFunNameShort := simpleFunName
					for i := len(simpleFunName) - 1; i > 0; i-- {
						if simpleFunName[i] == '/' {
							simpleFunNameShort = simpleFunName[i+1:]
							break
						}
					}

					head := fmt.Sprintf("%v:%v", short, line)

					log.Errorf("[%v]ExecuteWithTimeoutRetry(f=%v , time=%v , timeout= %v, suicide=%v)  caller :%v catch a err: %v", head, simpleFunNameShort, times, timeout, suicide, callFunNameShort, err)

					panic(err)
				}
				return err
			}
			times--
		} else {
			return nil
		}
	}
}

func ExecuteWithRetry(f SimpleFunc, times uint32, suicide bool) error {
	err := ExecuteWithTimeoutRetry(f, times, time.Second, suicide)
	if err != nil {
		funPc, fileName, line, ok := runtime.Caller(1)

		if !ok {
			fileName = "???"
			line = 0
		}

		//pcName := runtime.FuncForPC(pc).Name()

		short := fileName
		for i := len(fileName) - 1; i > 0; i-- {
			if fileName[i] == '/' {
				short = fileName[i+1:]
				break
			}
		}

		callFunName := runtime.FuncForPC(funPc).Name()
		callFunNameShort := callFunName
		for i := len(callFunName) - 1; i > 0; i-- {
			if callFunName[i] == '/' {
				callFunNameShort = callFunName[i+1:]
				break
			}
		}

		simpleFunName := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
		simpleFunNameShort := simpleFunName
		for i := len(simpleFunName) - 1; i > 0; i-- {
			if simpleFunName[i] == '/' {
				simpleFunNameShort = simpleFunName[i+1:]
				break
			}
		}

		head := fmt.Sprintf("%v:%v", short, line)

		log.Errorf("[%v]ExecuteWithRetry(f=%v , time=%v , suicide=%v)  caller :%v catch a err: %v", head, simpleFunNameShort, times, suicide, callFunNameShort, err)
	}
	return err
}

func (a *SwitchActionExecutor) LogSwitchEvent(step string, startAt time.Time, l string, err *error) {
	head := common.GetCallerInfo()

	spend := time.Since(startAt).Seconds()

	if err == nil || *err == nil {
		l = fmt.Sprintf("[%v]Success to [%v] cost: %v s", head, l, spend)
	} else {
		l = fmt.Sprintf("[%v]Failed to [%v],cost:%v s err :%v ", head, l, spend, (*err).Error())
	}

	stepInfo := common.SwitchStep{
		Name:        step,
		TimeElapsed: time.Since(startAt).String(),
		Timestamp:   startAt.String(),
		Log:         l,
	}

	a.Event.Steps = append(a.Event.Steps, stepInfo)
	log.Info(l)

	//去掉，无用信息太多，不易读懂
	oldRw := a.SwitchAction.OldRw
	newRw := a.SwitchAction.NewRw
	//
	oldRwMsg := fmt.Sprintf("Id:%s Pod:%s ep: %s", oldRw.CustID, oldRw.PodName, oldRw.Endpoint)
	newRwMsg := fmt.Sprintf("Id:%s Pod:%s ep: %s", newRw.CustID, newRw.PodName, newRw.Endpoint)
	rEvent := notify.BuildDBEventPrefixByInsV2(&a.SwitchAction.OldRw, common.RwEngine, oldRw.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("SwitchStep %s done, switch role (%s -> %s), message: %s , timestamp: %s spend:%s , ", step, oldRwMsg, newRwMsg, stepInfo.Log, stepInfo.Timestamp, stepInfo.TimeElapsed)
	rEvent.EventCode = notify.RedLineEventCode(string(notify.EventCode_SwitchTaskStepDone) + ":" + step)
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("LogSwitchEvent err: %v", sErr)
	}

}

func (a *SwitchActionExecutor) StartSwitchTask() error {
	startTs := time.Now()
	err := meta.GetMetaManager().CreateSwitchTask(a.SwitchAction)
	if err != nil {
		return errors.Wrapf(err, "Failed to create switchover task %v on k8s", a.SwitchAction)
	}

	a.Event.Action = a.SwitchAction
	a.Event.UUID = string(uuid.NewUUID())
	a.Event.StartAt = startTs
	l := fmt.Sprintf("start switch task uuid %s from %s to %s", a.Event.UUID, a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String())
	a.LogSwitchEvent("StartSwitchTask", startTs, l, nil)

	oldRw := a.SwitchAction.OldRw
	newRw := a.SwitchAction.NewRw

	rEvent := notify.BuildDBEventPrefixByInsV2(&oldRw, common.RwEngine, oldRw.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("instance %v switching task begin , po: %v", oldRw.CustID, oldRw.PodName)
	rEvent.EventCode = notify.EventCode_SwitchBegin_RwToRo
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("StartSwitchTask err: %v", sErr)
	}

	nEvent := notify.BuildDBEventPrefixByInsV2(&oldRw, common.RwEngine, oldRw.Endpoint)
	nEvent.Body.Describe = fmt.Sprintf("instance %v switching task begin , po: %v", newRw.CustID, newRw.PodName)
	nEvent.EventCode = notify.EventCode_SwitchBegin_RoToRw
	nEvent.Level = notify.EventLevel_INFO

	nErr := notify.MsgNotify.SendMsgToV2(&nEvent)
	if nErr != nil {
		log.Errorf("StartSwitchTask err: %v", nErr)
	}

	return nil
}

func (a *SwitchActionExecutor) FinishSwitchTask() error {
	startTs := time.Now()
	err := meta.GetMetaManager().FinishSwitchTask()
	if err != nil {
		return errors.Wrapf(err, "Failed to finish task on k8s")
	}

	oldRw := a.SwitchAction.OldRw
	newRw := a.SwitchAction.NewRw

	rEvent := notify.BuildDBEventPrefixByInsV2(&oldRw, common.RwEngine, oldRw.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("Instance %v switching done, po: %v", oldRw.CustID, oldRw.PodName)
	rEvent.EventCode = notify.EventCode_SwitchTaskDone_RwToRo
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("FinishSwitchTask err: %v", sErr)
	}

	nEvent := notify.BuildDBEventPrefixByInsV2(&oldRw, common.RwEngine, oldRw.Endpoint)
	nEvent.Body.Describe = fmt.Sprintf("Instance %v switching done, po: %v", newRw.CustID, newRw.PodName)
	nEvent.EventCode = notify.EventCode_SwitchTaskDone_RoToRw
	nEvent.Level = notify.EventLevel_INFO

	nErr := notify.MsgNotify.SendMsgToV2(&nEvent)
	if nErr != nil {
		log.Errorf("FinishSwitchTask err: %v", nErr)
	}

	l := fmt.Sprintf("finish switch task uuid %s from %s to %s", a.Event.UUID, a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String())
	a.LogSwitchEvent("FinishSwitchTask", startTs, l, nil)
	a.Event.SwitchTime = time.Since(a.Event.StartAt)

	err = meta.GetMetaManager().AddSwitchEvent(&a.Event)
	if err != nil {
		return errors.Wrapf(err, "Failed to add switch event %v", a.Event)
	}
	return nil
}

func (a *SwitchActionExecutor) RollbackRwVip() error {
	startTs := time.Now()

	err, l := resource.GetResourceManager().GetOperatorClient().SwitchVip(a.SwitchAction.NewRw, a.SwitchAction.OldRw, a.SwitchAction.Type)

	a.LogSwitchEvent("RollbackRwVip", startTs, l, &err)

	return err
}

func (a *SwitchActionExecutor) SwitchRwVip() error {
	startTs := time.Now()

	err, l := resource.GetResourceManager().GetOperatorClient().SwitchVip(a.SwitchAction.OldRw, a.SwitchAction.NewRw, a.SwitchAction.Type)

	a.LogSwitchEvent("SwitchRwVip", startTs, l, &err)

	return err
}

func (a *SwitchActionExecutor) SwitchStore() error {
	oldRw := &a.SwitchAction.OldRw
	newRw := &a.SwitchAction.NewRw

	rEvent := notify.BuildDBEventPrefixByInsV2(oldRw, common.RwEngine, oldRw.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("Instance %v switch store begin, po: %v", oldRw.CustID, oldRw.PodName)
	rEvent.EventCode = notify.EventCode_SwitchStoreBegin_RwToRo
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("send begin store switch err: %v", sErr)
	}

	nEvent := notify.BuildDBEventPrefixByInsV2(newRw, common.RoEngine, newRw.Endpoint)
	nEvent.Body.Describe = fmt.Sprintf("instance %v switch store begin, po: %v", newRw.CustID, newRw.PodName)
	nEvent.EventCode = notify.EventCode_SwitchStoreBegin_RoToRw
	nEvent.Level = notify.EventLevel_INFO

	nErr := notify.MsgNotify.SendMsgToV2(&nEvent)
	if nErr != nil {
		log.Errorf("send begin store switch err: %v", nErr)
	}

	startTs := time.Now()
	l := fmt.Sprintf("switch rw store from %s to %s", oldRw.String(), newRw.String())

	err, _ := resource.GetResourceManager().GetOperatorClient().SwitchStore(a.SwitchAction.OldRw, a.SwitchAction.NewRw)
	a.LogSwitchEvent("SwitchPolarStore", startTs, l, &err)

	return err
}

func (a *SwitchActionExecutor) RollbackStore() error {
	oldRw := &a.SwitchAction.OldRw
	newRw := &a.SwitchAction.NewRw

	rEvent := notify.BuildDBEventPrefixByInsV2(oldRw, common.RwEngine, oldRw.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("Rollback store begin. rw->ro , pod: %+v", oldRw.PodName)
	rEvent.EventCode = notify.EventCode_SwitchTaskRollBackStore_RwToRo
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("rollback begin store switch err: %v", sErr)
	}

	nEvent := notify.BuildDBEventPrefixByInsV2(newRw, common.RoEngine, newRw.Endpoint)
	nEvent.Body.Describe = fmt.Sprintf("Rollback store begin. ro->rw , pod: %+v", newRw.PodName)
	nEvent.EventCode = notify.EventCode_SwitchTaskRollBackStore_RoToRw
	nEvent.Level = notify.EventLevel_INFO

	nErr := notify.MsgNotify.SendMsgToV2(&nEvent)
	if nErr != nil {
		log.Errorf("rollback begin store switch err: %v", nErr)
	}

	startTs := time.Now()
	l := fmt.Sprintf("rollback rw store from %s to %s", newRw.String(), oldRw.String())

	err, _ := resource.GetResourceManager().GetOperatorClient().SwitchStore(a.SwitchAction.NewRw, a.SwitchAction.OldRw)
	a.LogSwitchEvent("RollbackPolarStore", startTs, l, &err)

	return err
}

func (a *SwitchActionExecutor) PromoteNewRw(engineType common.EngineType) error {
	var err error
	startTs := time.Now()
	l := fmt.Sprintf("promote %s", a.SwitchAction.NewRw.String())
	defer a.LogSwitchEvent("PromoteNewRw", startTs, l, &err)

	newRw := a.SwitchAction.NewRw
	newEngine := resource.GetResourceManager().GetEngineManager(newRw.Endpoint)
	engPod := newEngine.InsSpec()
	if engPod.PodName != newRw.PodName {
		log.Warnf("PromoteNewRw found engPod info(%s/%s@%s) is difference newRw. set to newRw pod info(%s/%s@%s)",
			engPod.PodID, engPod.PodName, engPod.HostName,
			newRw.PodID, newRw.PodName, newRw.HostName)
		engPod.PodName = newRw.PodName
		engPod.PodID = newRw.PodID
		engPod.HostName = newRw.HostName
	}
	newRos := append(a.SwitchAction.OldRo, a.SwitchAction.OldRw)
	err = newEngine.Promote(engineType, newRos, a.Online)
	if err != nil {
		return errors.Wrapf(err, "Failed to promote %s", a.SwitchAction.NewRw.String())
	}

	return nil
}

func (a *SwitchActionExecutor) PromoteNewStandby(masterRw common.InsSpec) error {
	var err error
	startTs := time.Now()
	l := fmt.Sprintf("promote standby %s", a.SwitchAction.NewRw.String())
	defer a.LogSwitchEvent("PromoteNewRw", startTs, l, &err)

	newEngine := resource.GetResourceManager().GetEngineManager(a.SwitchAction.NewRw.Endpoint)
	err = newEngine.Demote(common.StandbyEngine, masterRw, false, false)
	if err != nil {
		return errors.Wrapf(err, "Failed to promote standby %s", a.SwitchAction.NewRw.String())
	}

	return nil
}

func (a *SwitchActionExecutor) DemoteOldRw(engineType common.EngineType) error {
	var err error
	startTs := time.Now()
	l := fmt.Sprintf("demote old rw %s to %s", a.SwitchAction.OldRw.String(), engineType)
	defer a.LogSwitchEvent("DemoteOldRw", startTs, l, &err)

	oldEngine := resource.GetResourceManager().GetEngineManager(a.SwitchAction.OldRw.Endpoint)
	err = oldEngine.Demote(engineType, a.SwitchAction.NewRw, false, !a.SwitchAction.Manual)
	if err != nil {
		return errors.Wrapf(err, "Failed to demote %s", a.SwitchAction.OldRw.String())
	}

	return nil
}

func (a *SwitchActionExecutor) WaitRwReady() error {
	startTs := time.Now()
	c := 0
	errCount := 0
	var err error = nil
	for {
		c++
		db := resource.GetResourceManager().GetEngineConn(a.SwitchAction.NewRw.Endpoint)
		var id int64
		_, err = db.Query(pg.Scan(&id), common.PolarDBRwHealthCheckSQL)
		if err != nil {
			if strings.Contains(err.Error(), common.EngineStart) {
				errCount = 0
			} else if strings.Contains(err.Error(), common.EngineReadOnly) {
				errCount = 0
			} else {
				errCount++
			}
			if c%10 == 0 {
				log.Info("Rw still not ready", db, err, time.Since(startTs))
			}
			resource.GetResourceManager().ResetEngineConn(a.SwitchAction.NewRw.Endpoint)
			if errCount > 100 {
				log.Warnf("Rw still not ready with err %s more than 10s", err.Error())
				break
			}
		} else {
			break
		}
		if time.Since(startTs).Seconds() > 600 {
			log.Warnf("Failed to wait rw ready")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	rwTotalSpend := time.Now().Sub(a.Event.StartAt).Seconds()

	l := fmt.Sprintf("wait rw %s ready[total:=%v s]", a.SwitchAction.NewRw.String(), rwTotalSpend)
	a.LogSwitchEvent("WaitRwReady", startTs, l, &err)

	return err
}

func (a *SwitchActionExecutor) StopAllRo() error {

	waitGroup := &sync.WaitGroup{}

	startTs := time.Now()

	if !a.SwitchAction.Manual {
		waitGroup.Add(1)
		go func() {
			engine := resource.GetResourceManager().GetEngineManager(a.SwitchAction.OldRw.Endpoint)
			err := engine.Stop(resource.ImmediateStop, 0)
			if err != nil {
				log.Warnf("Failed to stop ro %s err %s", a.SwitchAction.OldRw.String(), err.Error())
			}
			waitGroup.Done()
		}()
	}
	for _, ro := range a.SwitchAction.OldRo {
		waitGroup.Add(1)
		go func(ro common.InsSpec) {
			engine := resource.GetResourceManager().GetEngineManager(ro.Endpoint)
			if engine != nil {
				err := engine.Stop(resource.FastStop, 1)
				if err != nil {
					log.Warnf("Failed to stop ro %s err %s", ro.String(), err.Error())
				}
			} else {
				log.Warnf("Failed to get engine manager %s", ro.Endpoint.String())
			}
			waitGroup.Done()
		}(ro)
	}

	waitGroup.Wait()

	l := fmt.Sprintf("stop all ro")
	if a.SwitchAction.Manual {
		a.LogSwitchEvent("StopAllRo", startTs, l, nil)
	} else {
		log.Infof(l + " cost " + time.Since(startTs).String())
	}
	return nil
}

func (a *SwitchActionExecutor) StopOldRw() error {
	var l string
	startTs := time.Now()
	l = fmt.Sprintf("stop old rw %s", a.SwitchAction.OldRw.String())

	oldEngine := resource.GetResourceManager().GetEngineManager(a.SwitchAction.OldRw.Endpoint)
	err := oldEngine.Stop(resource.FastStop, 0)

	a.LogSwitchEvent("StopOldRw", startTs, l, &err)
	return err
}

func (a *SwitchActionExecutor) StartOldRw() error {
	var l string
	startTs := time.Now()
	l = fmt.Sprintf("start old rw %s", a.SwitchAction.OldRw.String())
	oldEngine := resource.GetResourceManager().GetEngineManager(a.SwitchAction.OldRw.Endpoint)
	err := oldEngine.Promote(common.RwEngine, nil, false)

	a.LogSwitchEvent("StartOldRw", startTs, l, &err)
	return nil
}

func (a *SwitchActionExecutor) ReConfigRo() error {
	var l string
	for _, ro := range a.SwitchAction.OldRo {
		startTs := time.Now()
		oldEngine := resource.GetResourceManager().GetEngineManager(ro.Endpoint)
		if oldEngine != nil {
			l = fmt.Sprintf("reConfig ro %s", ro.String())
			err := oldEngine.Demote(common.RoEngine, a.SwitchAction.NewRw, a.Online, false)
			a.LogSwitchEvent("ReConfigRo", startTs, l, &err)
		} else {
			log.Warnf("Failed to get engine manager %s", ro.Endpoint.String())
		}
	}

	return nil
}

func (a *SwitchActionExecutor) ReConfigStandby() error {
	var l string
	for _, standby := range a.SwitchAction.OldStandby {
		startTs := time.Now()
		l = fmt.Sprintf("reConfig standby %s", standby.String())

		oldEngine := resource.GetResourceManager().GetEngineManager(standby.Endpoint)
		err := oldEngine.Demote(common.StandbyEngine, a.SwitchAction.NewRw, false, false)

		a.LogSwitchEvent("ReConfigStandby", startTs, l, &err)
	}

	return nil
}

func (a *SwitchActionExecutor) Checkpoint() error {
	var l string
	startTs := time.Now()
	info := resource.GetClusterManagerConfig()
	db := pg.Connect(&pg.Options{
		Addr:         a.SwitchAction.OldRw.Endpoint.String(),
		User:         info.Account.AuroraUser,
		Password:     info.Account.AuroraPassword,
		Database:     common.DefaultDatabase,
		PoolSize:     1,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second * 180,
		WriteTimeout: time.Second * 180,
	})
	defer func() {
		er := db.Close()
		if er != nil {
			//
		}
	}()
	sql := `checkpoint`
	_, err := db.Exec(sql)
	if err != nil {
		err = errors.Wrapf(err, "Failed to checkpoint on %s", a.SwitchAction.OldRw.String())
	}
	l = fmt.Sprintf("checkpoint on %s", a.SwitchAction.OldRw.String())

	a.LogSwitchEvent("Checkpoint", startTs, l, &err)

	return err
}

func (a *SwitchActionExecutor) CheckEngineVersion() error {
	startTs := time.Now()
	a.Online = false
	reason := ""
	a.PersistSlot = false

	db := resource.GetResourceManager().GetMetricsConn(a.SwitchAction.NewRw.Endpoint)
	// 1. clean old replication slot
	type PgSetting struct {
		Name    string
		Setting string
	}
	settingMap := map[string]string{}
	var settings []PgSetting

	sql := "select name, setting from pg_settings where name in ('polar_release_date')"
	_, err := db.Query(&settings, sql)
	if err != nil {
		log.Warnf("Failed to query settings %s err %s", sql, err.Error())
	}
	for _, setting := range settings {
		settingMap[setting.Name] = setting.Setting
		log.Infof("setting %s %s", setting.Name, setting.Setting)
	}
	if *common.EnableOnlinePromote {
		if v, exist := settingMap["polar_release_date"]; exist && v >= "20201015" {
			a.Online = true
			reason = "release_date " + v
		}
	}

	type Config struct {
		PolarEnablePersistedPhysicalSlot string
	}
	var c Config
	sql = "show polar_enable_persisted_physical_slot"
	_, err = db.Query(&c, sql)
	if err != nil {
		log.Warnf("Failed to query settings %s err %s", sql, err.Error())
	}
	if c.PolarEnablePersistedPhysicalSlot == "true" || c.PolarEnablePersistedPhysicalSlot == "on" {
		a.PersistSlot = true
	}

	if v, exist := a.Event.Reason.Metrics[common.MetricsOnlinePromote]; exist {
		a.Online, _ = v.(bool)
	}

	l := fmt.Sprintf("Success to check setting online:%v persistSlot:%v rease:%s cost %s", a.Online, a.PersistSlot, reason, time.Since(startTs).String())
	a.Event.Steps = append(a.Event.Steps, common.SwitchStep{
		Name:        "CheckVersion",
		TimeElapsed: time.Since(startTs).String(),
		Timestamp:   startTs.String(),
		Log:         l,
	})
	log.Info(l)

	return nil
}

func (a *SwitchActionExecutor) CleanAndCreateSlotForOnlinePromote() error {
	var err error = nil
	var l string
	startTs := time.Now()
	db := resource.GetResourceManager().GetMetricsConn(a.SwitchAction.NewRw.Endpoint)
	oldSlot := resource.GenerateSlotName(common.RoEngine, &a.SwitchAction.NewRw)
	newSlot := resource.GenerateSlotName(common.RoEngine, &a.SwitchAction.OldRw)

	for {
		// 1. clean old replication slot
		sql := common.InternalMarkSQL + `select pg_drop_replication_slot('` + oldSlot + `')`
		_, err = db.Exec(sql)
		if err != nil {
			if !strings.Contains(err.Error(), "is active") {
				l = fmt.Sprintf("Failed to drop slot %s for %s", oldSlot, a.SwitchAction.NewRw.String())
				err = errors.Wrapf(err, "Failed to drop slot with sql %s err %s", sql, err.Error())
				break
			}
		}

		// 2. create new replication slot
		sql = common.InternalMarkSQL + `select pg_create_physical_replication_slot('` + newSlot + `', true)`
		_, err = db.Exec(sql)
		if err != nil {
			if !strings.Contains(err.Error(), common.DUPLICATE_OBJECT_ERROR) {
				l = fmt.Sprintf("Failed to create slot %s for %s", newSlot, a.SwitchAction.OldRw.String())
				err = errors.Wrapf(err, "Failed to create slot with sql %s for %s", sql, a.SwitchAction.OldRw.String())
				break
			}
		}
		l = fmt.Sprintf("drop old slot %s create new slot %s cost %s", oldSlot, newSlot, time.Since(startTs).String())
		break
	}
	a.LogSwitchEvent("CleanAndCreateSlot", startTs, l, &err)

	return err
}

func (a *SwitchActionExecutor) CleanOldRwSlot() error {
	var err error = nil
	startTs := time.Now()
	info := resource.GetClusterManagerConfig()
	db := pg.Connect(&pg.Options{
		Addr:         a.SwitchAction.OldRw.Endpoint.String(),
		User:         info.Account.AuroraUser,
		Password:     info.Account.AuroraPassword,
		Database:     "polardb_admin",
		PoolSize:     1,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second * 60,
		WriteTimeout: time.Second * 60,
	})
	defer db.Close()

	l := fmt.Sprintf("clean %s pysical slot", a.SwitchAction.OldRw.String())
	defer a.LogSwitchEvent("CleanOldRwSlot", startTs, l, &err)

	// 1. clean old replication slot
	type ReplicationSlot struct {
		SlotName string
	}
	var oldSlots []ReplicationSlot
	sql := common.InternalMarkSQL + "select slot_name from pg_replication_slots where slot_type='physical'"
	_, err = db.Query(&oldSlots, sql)
	if err != nil {
		return errors.Wrapf(err, "Failed to query old slot with sql %s", sql)
	}
	for _, oldSlot := range oldSlots {
		sql := common.InternalMarkSQL + `select pg_drop_replication_slot('` + oldSlot.SlotName + `')`
		_, err = db.Exec(sql)
		if err != nil {
			if !strings.Contains(err.Error(), "is active") {
				return errors.Wrapf(err, "Failed to drop slot with sql %s", sql)
			}
		}
		log.Infof("Success to drop slot %s", oldSlot.SlotName)
	}

	return nil
}

func (a *SwitchActionExecutor) CleanAndCreateSlot() error {
	var err error = nil
	startTs := time.Now()
	info := resource.GetClusterManagerConfig()
	db := pg.Connect(&pg.Options{
		Addr:         a.SwitchAction.NewRw.Endpoint.String(),
		User:         info.Account.AuroraUser,
		Password:     info.Account.AuroraPassword,
		Database:     common.DefaultDatabase,
		PoolSize:     1,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second * 180,
		WriteTimeout: time.Second * 180,
	})
	defer db.Close()

	l := fmt.Sprintf("clean slot and create slot")
	defer a.LogSwitchEvent("CleanAndCreateSlot", startTs, l, &err)

	// 1. clean old replication slot
	type ReplicationSlot struct {
		SlotName string
	}
	var oldSlots []ReplicationSlot
	sql := common.InternalMarkSQL + "select slot_name from pg_replication_slots where slot_type='physical'"
	_, err = db.Query(&oldSlots, sql)
	if err != nil {
		return errors.Wrapf(err, "Failed to query old slot with sql %s", sql)
	}
	for _, oldSlot := range oldSlots {
		sql := common.InternalMarkSQL + `select pg_drop_replication_slot('` + oldSlot.SlotName + `')`
		_, err = db.Exec(sql)
		if err != nil {
			if !strings.Contains(err.Error(), "is active") {
				return errors.Wrapf(err, "Failed to drop slot with sql %s", sql)
			}
		}
		log.Infof("Success to drop slot %s", oldSlot.SlotName)
	}

	// 2. create new replication slot
	slotRo := a.SwitchAction.OldRo
	if a.SwitchAction.Type != common.MasterStandbySwitch {
		slotRo = append(slotRo, a.SwitchAction.OldRw)
	}
	for _, ro := range slotRo {
		slotName := resource.GenerateSlotName(common.RoEngine, &ro)
		sql := common.InternalMarkSQL + `select pg_create_physical_replication_slot('` + slotName + `')`
		_, err = db.Exec(sql)
		if err != nil {
			if !strings.Contains(err.Error(), common.DUPLICATE_OBJECT_ERROR) {
				return errors.Wrapf(err, "Failed to create slot with sql %s on %s", sql, ro.String())
			}
		}
		log.Infof("Success to create slot %s for %s", slotName, ro.String())
	}
	slotStandby := a.SwitchAction.OldStandby
	if a.SwitchAction.Type == common.MasterStandbySwitch {
		slotStandby = append(slotStandby, a.SwitchAction.OldRw)
	}
	for _, standby := range slotStandby {
		slotName := resource.GenerateSlotName(common.StandbyEngine, &standby)
		sql := common.InternalMarkSQL + `select pg_create_physical_replication_slot('` + slotName + `')`
		_, err = db.Exec(sql)
		if err != nil {
			if !strings.Contains(err.Error(), common.DUPLICATE_OBJECT_ERROR) {
				return errors.Wrapf(err, "Failed to create slot with sql %s on %s", sql, standby.String())
			}
		}
		log.Infof("Success to create slot %s for %s", slotName, standby.String())
	}

	return nil
}

func (a *SwitchActionExecutor) UpdateK8sStatus() error {
	var err error = nil
	startTs := time.Now()
	operatorClient := resource.GetResourceManager().GetOperatorClient()
	l := fmt.Sprintf("update k8s status from %s to %s", a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String())
	defer a.LogSwitchEvent("UpdateK8sStatus", startTs, l, &err)

	// Update Old RW detector to RO/Standby detector
	err = operatorClient.UpdateEngineStatus(a.SwitchAction.OldRw, common.EnginePhaseStarting, "")
	if err != nil {
		return err
	}
	// Update New RW detector to Rw/Standby-rw detector
	if a.SwitchAction.Type == common.MasterStandbySwitch {
		err = operatorClient.UpdateEngineStatus(a.SwitchAction.NewRw, common.EnginePhaseStarting, "")
		if err != nil {
			return err
		}
	} else {
		err = operatorClient.UpdateEngineStatus(a.SwitchAction.NewRw, common.EnginePhaseStarting, "")
		if err != nil {
			return err
		}
	}
	// Update Ro detector
	for _, ro := range a.SwitchAction.OldRo {
		err = operatorClient.UpdateEngineStatus(ro, common.EnginePhaseStarting, "")
		if err != nil {
			return err
		}
	}
	for _, standby := range a.SwitchAction.OldStandby {
		err = operatorClient.UpdateEngineStatus(standby, common.EnginePhaseStarting, "")
		if err != nil {
			return err
		}
	}
	if a.SwitchAction.OldRw.ClusterID == a.SwitchAction.NewRw.ClusterID {
		if a.SwitchAction.Type == common.MasterReplicaSwitch {
			err = operatorClient.MasterReplicaMetaSwitch(a.SwitchAction.OldRw, a.SwitchAction.NewRw, a.SwitchAction.NewRw.ClusterID)
			if err != nil {
				return errors.Wrapf(err, "Failed to update k8s status from %s to %s", a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String())
			}
		} else {
			err = operatorClient.MasterReplicaMetaSwitch(a.SwitchAction.OldRw, a.SwitchAction.NewRw, a.SwitchAction.NewRw.ClusterID)
			if err != nil {
				return errors.Wrapf(err, "Failed to update k8s status from %s to %s", a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String())
			}
		}
	} else {
		err = operatorClient.MasterStandbyMetaSwitch(a.SwitchAction.OldRw, a.SwitchAction.NewRw)
		if err != nil {
			return errors.Wrapf(err, "Failed to update k8s status from %s to %s", a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String())
		}
	}
	return err
}

func (a *SwitchActionExecutor) DisableSwitchInsStatus(m *meta.MetaManager, s *status.StatusManager) {
	startTs := time.Now()
	ts, exist := s.ClusterStatus.SubCluster[a.SwitchAction.OldRw.ClusterID].EngineStatus[a.SwitchAction.OldRw.Endpoint][common.EngineEventCategory].Value[common.EventTimestamp]
	if exist {
		tagMap := map[string]string{
			common.EventTimestamp: strconv.FormatInt(ts.(int64), 10),
		}
		m.AddEngineTag(&a.SwitchAction.OldRw, tagMap)
	}

	s.DeleteEngineStatus(a.SwitchAction.OldRw.Endpoint)
	s.DeleteEngineStatus(a.SwitchAction.NewRw.Endpoint)
	for _, ro := range a.SwitchAction.OldRo {
		s.DeleteEngineStatus(ro.Endpoint)
	}
	for _, standby := range a.SwitchAction.OldStandby {
		s.DeleteEngineStatus(standby.Endpoint)
	}

	l := fmt.Sprint("disable oldRW & newRw status.")
	a.LogSwitchEvent("DisableSwitchInsStatus", startTs, l, nil)
}

func (a *SwitchActionExecutor) EnableSwitchInsStatus(metaManager *meta.MetaManager, statusManager *status.StatusManager) {
	s := statusManager.ClusterStatus
	m := metaManager.ClusterMeta
	// update old rw & new rw status
	startTs := time.Now()
	s.StartAt = startTs

	metaManager.SetInsPhase(&a.SwitchAction.NewRw, common.EnginePhaseStarting)
	metaManager.SetInsPhase(&a.SwitchAction.OldRw, common.EnginePhaseStarting)

	a.SwitchAction.NewRw.RecoverySizeMB = a.SwitchAction.RecoverySize / (1024 * 1024)
	a.SwitchAction.OldRw.RecoverySizeMB = a.SwitchAction.RecoverySize / (1024 * 1024)
	var rwSpec *common.InsSpec
	if a.SwitchAction.Type == common.MasterReplicaSwitch {
		// 切换成功
		if m.SubCluster[a.SwitchAction.NewRw.ClusterID].RwEndpoint == a.SwitchAction.NewRw.Endpoint {
			statusManager.CreateOrUpdateEngineStatus(a.SwitchAction.OldRw.ClusterID, &a.SwitchAction.OldRw, common.RoEngine, &a.SwitchAction.NewRw)
			statusManager.CreateOrUpdateEngineStatus(a.SwitchAction.NewRw.ClusterID, &a.SwitchAction.NewRw, common.RwEngine, &a.SwitchAction.NewRw)
			rwSpec = &a.SwitchAction.NewRw
			// 切换失败
		} else {
			statusManager.CreateOrUpdateEngineStatus(a.SwitchAction.NewRw.ClusterID, &a.SwitchAction.NewRw, common.RoEngine, &a.SwitchAction.OldRw)
			statusManager.CreateOrUpdateEngineStatus(a.SwitchAction.OldRw.ClusterID, &a.SwitchAction.OldRw, common.RwEngine, &a.SwitchAction.OldRw)
			rwSpec = &a.SwitchAction.OldRw
		}
	} else if a.SwitchAction.Type == common.MasterStandbySwitch {
		if m.MasterClusterID == a.SwitchAction.NewRw.ClusterID {
			statusManager.CreateOrUpdateEngineStatus(a.SwitchAction.OldRw.ClusterID, &a.SwitchAction.OldRw, common.StandbyEngine, &a.SwitchAction.NewRw)
			statusManager.CreateOrUpdateEngineStatus(a.SwitchAction.NewRw.ClusterID, &a.SwitchAction.NewRw, common.RwEngine, &a.SwitchAction.NewRw)
			rwSpec = &a.SwitchAction.NewRw
		} else {
			statusManager.CreateOrUpdateEngineStatus(a.SwitchAction.NewRw.ClusterID, &a.SwitchAction.NewRw, common.StandbyEngine, &a.SwitchAction.OldRw)
			statusManager.CreateOrUpdateEngineStatus(a.SwitchAction.OldRw.ClusterID, &a.SwitchAction.OldRw, common.RwEngine, &a.SwitchAction.OldRw)
			rwSpec = &a.SwitchAction.OldRw
		}
	} else if a.SwitchAction.Type == common.StandbyReplicaSwitch {
		if m.SubCluster[a.SwitchAction.NewRw.ClusterID].RwEndpoint == a.SwitchAction.NewRw.Endpoint {
			statusManager.CreateOrUpdateEngineStatus(a.SwitchAction.NewRw.ClusterID, &a.SwitchAction.NewRw, common.StandbyEngine, &a.SwitchAction.Master)
			statusManager.CreateOrUpdateEngineStatus(a.SwitchAction.OldRw.ClusterID, &a.SwitchAction.OldRw, common.RoEngine, &a.SwitchAction.NewRw)
		} else {
			statusManager.CreateOrUpdateEngineStatus(a.SwitchAction.NewRw.ClusterID, &a.SwitchAction.NewRw, common.RoEngine, &a.SwitchAction.OldRw)
			statusManager.CreateOrUpdateEngineStatus(a.SwitchAction.OldRw.ClusterID, &a.SwitchAction.OldRw, common.StandbyEngine, &a.SwitchAction.Master)
		}
		rwSpec = &a.SwitchAction.Master
	} else {
		panic(fmt.Errorf("invalid switch type %v", a.SwitchAction.Type))
	}
	// Update Ro status, master standby switch has not affect ro
	if a.SwitchAction.Type != common.MasterStandbySwitch {
		for _, ro := range a.SwitchAction.OldRo {
			err := metaManager.SetInsPhase(&ro, common.EnginePhaseStarting)
			if err != nil {
				log.Warnf("Failed to set ins %s phase %s", ro.String(), common.EnginePhaseStarting)
				continue
			}

			ro.RecoverySizeMB = a.SwitchAction.RecoverySize / (1024 * 1024)
			if m.SubCluster[a.SwitchAction.NewRw.ClusterID].RwEndpoint == a.SwitchAction.NewRw.Endpoint {
				statusManager.CreateOrUpdateEngineStatus(ro.ClusterID, &ro, common.RoEngine, &a.SwitchAction.NewRw)
			} else {
				statusManager.CreateOrUpdateEngineStatus(ro.ClusterID, &ro, common.RoEngine, &a.SwitchAction.OldRw)
			}
		}
	}

	// Update standby status
	for _, standby := range a.SwitchAction.OldStandby {
		metaManager.SetInsPhase(&standby, common.EnginePhaseStarting)

		statusManager.CreateOrUpdateEngineStatus(standby.ClusterID, &standby, common.StandbyEngine, rwSpec)
	}

	// update proxy status
	for endpoint, insSpec := range metaManager.ProxySpec.InstanceSpec {
		log.Infof("ClusterManager reset proxy %s", endpoint.String())
		statusManager.CreateOrUpdateProxyStatus(insSpec, rwSpec)
		statusManager.ClusterStatus.Proxy[endpoint] = map[common.EventCategory]status.State{
			common.EngineEventCategory: status.State{Name: status.ProxyStatusInit},
		}
	}

	statusManager.CreateOrUpdateCmStatus(rwSpec)

	l := fmt.Sprint("enable oldRW & newRw status.")
	a.LogSwitchEvent("EnableSwitchInsStatus", startTs, l, nil)
}
