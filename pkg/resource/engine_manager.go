package resource

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"strings"
	"time"
)

type EngineStopType string

const (
	FastStop      EngineStopType = "fast"
	ImmediateStop EngineStopType = "immediate"
	SmartStop     EngineStopType = "smart"

	EngineContainerName         string = "engine"
	ManagerContainerName        string = "manager"
	ClusterManagerContainerName string = "clustermanager"
)

type EngineManager interface {
	InsSpec() *common.InsSpec
	Promote(engineType common.EngineType, newRos []common.InsSpec, online bool) error
	Demote(engineType common.EngineType, newRw common.InsSpec, online bool, force bool) error
	Stop(stopType EngineStopType, times int) error
	Start(times int) error
	ExecSQL(sql string, timeout time.Duration) (string, string, error)
	LockReadonly(times int) error
	UnlockReadonly(times int) error
	SetSyncMode(standby *common.InsSpec, mode common.SyncType) error
	UpdateConf(settings []common.FileSetting) error
}

type PolarDBEngine struct {
	Spec            common.InsSpec
	NamespaceName   string
	ReplicaUser     string
	ReplicaPassword string
	Executor        CommandExecutor
}

func (e *PolarDBEngine) InsSpec() *common.InsSpec {
	return &e.Spec
}

func (e *PolarDBEngine) onlinePromote(engineType common.EngineType, newRos []common.InsSpec) error {
	log.Infof("Starting Online Promote %s to rw", e.Spec.String())

	rEvent := notify.BuildDBEventPrefixByInsV2(&e.Spec, common.RwEngine, e.Spec.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("instance %v promote online , new rw po: %v", e.Spec.CustID, e.Spec.PodName)
	rEvent.EventCode = notify.EventCode_PromoteOnline
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("onlinePromote event err: %v", sErr)
	}

	var command []string
	if e.Spec.IsRPMDeploy() {
		hasSudo := ""
		if *common.WorkUser != "root" {
			hasSudo = "sudo "
		}
		command = []string{hasSudo + "su - " + e.Spec.EngineUser + " -c \"/usr/local/polardb_o_current/bin/pg_ctl -D " + e.Spec.DataPath + " -W promote\""}
	} else {
		command = []string{"bash", "-c", "su - postgres -c \"export PATH=/u01/polardb_pg/bin:/u01/polardb_ora/bin:$PATH; pg_ctl promote -D /data -t 5;\""}
	}

	times := 0
	for {
		if times >= 5 {
			log.Infof("Online Promote try %d times, skip it", times)
			return nil
		}
		times++
		stdout, stderr, err := e.Executor.Exec(command, 10, 15*time.Second)
		if err != nil {
			log.Warn("Failed to online promote", stdout, stderr, err.Error())
			if !strings.Contains(stderr, "did not promote in time") {
				return e.offlinePromote(engineType, newRos)
			}
		} else {
			log.Infof("Online promote return %s/%s", stdout, stderr)
			return nil
		}
	}

	return nil
}

func (e *PolarDBEngine) offlinePromote(engineType common.EngineType, newRos []common.InsSpec) error {
	log.Infof("Starting Offline Promote %s to rw", e.Spec.String())

	rEvent := notify.BuildDBEventPrefixByInsV2(&e.Spec, common.RwEngine, e.Spec.Endpoint)
	rEvent.Body.Describe = fmt.Sprintf("instance %v promote offline , new rw po: %v", e.Spec.CustID, e.Spec.PodName)
	rEvent.EventCode = notify.EventCode_PromoteOffLine
	rEvent.Level = notify.EventLevel_INFO

	sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
	if sErr != nil {
		log.Errorf("offlinePromote event err: %v", sErr)
	}

	var command []string
	if e.Spec.IsRPMDeploy() {
		hasSudo := ""
		if *common.WorkUser != "root" {
			hasSudo = "sudo "
		}
		command = []string{hasSudo + "su - " + e.Spec.EngineUser + " -c \"rm -f " + e.Spec.DataPath + "/recovery.conf && touch " + e.Spec.DataPath + "/recovery.done\""}
	} else {
		command = []string{"bash", "-c", "su - postgres -c \"rm -f /data/recovery.conf && touch /data/recovery.done\""}
	}

	stdout, stderr, err := e.Executor.Exec(command, 5, 30*time.Second)
	if err != nil {
		log.Warn("Failed to config recovery.conf", stdout, stderr)
		return err
	}

	if err := e.Stop(FastStop, 2); err != nil {
		log.Warnf("Failed to fast stop, err=%s, try immediate stop", err.Error())
		if err = e.Stop(ImmediateStop, 5); err != nil {
			log.Warnf("Failed to immediate stop, err=%s, skip it", err.Error())
			return err
		}
	}
	if err = e.Start(10); err != nil {
		log.Warnf("Failed to start, err=%s, skip it", err.Error())
	}

	return nil
}

func (e *PolarDBEngine) Promote(engineType common.EngineType, newRos []common.InsSpec, online bool) error {
	head := common.GetCallerInfo()
	now := time.Now()
	defer func() {
		spend := time.Now().Sub(now).Seconds()
		log.Infof("[%v]PolarDBEngine Promote[online=%v] action spend: %v s , engineType:%v", head, online, spend, engineType)
	}()
	if online {
		return e.onlinePromote(engineType, newRos)
	} else {
		return e.offlinePromote(engineType, newRos)
	}

	return nil
}

func (e *PolarDBEngine) Demote(engineType common.EngineType, newRw common.InsSpec, online bool, force bool) error {
	log.Infof("Starting Demote %s to %s", e.Spec.String(), engineType)

	var mode string
	primarySlotName := GenerateSlotName(engineType, &e.Spec)

	if engineType == common.RoEngine {
		mode = "polar_replica='on'"
	} else if engineType == common.StandbyEngine {
		mode = "standby_mode='on'"
	} else if engineType == common.DataMax {
		mode = "polar_datamax_mode = 'standalone'"
	} else {
		return errors.Errorf("Invalid engine type %s", engineType)
	}

	primaryConnInfo := `host\\=` + newRw.Endpoint.Host + ` port\\=` + newRw.Endpoint.Port + ` user\\=` + e.ReplicaUser +
		` password\\=` + e.ReplicaPassword + ` application_name\\=` + primarySlotName
	var command []string

	if e.Spec.IsRPMDeploy() {
		primaryConnInfo = `host=` + newRw.Endpoint.Host + ` port=` + newRw.Endpoint.Port + ` user=` + e.ReplicaUser +
			` password=` + e.ReplicaPassword + ` application_name=` + primarySlotName
		if *common.WorkUser == "root" {
			command = []string{`echo "` + mode + `" > ` + e.Spec.DataPath +
				`/recovery.conf; echo "recovery_target_timeline='latest'" >> ` +
				e.Spec.DataPath + `/recovery.conf;` + `echo "primary_conninfo='` + primaryConnInfo +
				`'" >> ` + e.Spec.DataPath + `/recovery.conf; echo "primary_slot_name='` + primarySlotName + `'" >> ` +
				e.Spec.DataPath + `/recovery.conf; chown ` + e.Spec.EngineUser + ` ` + e.Spec.DataPath +
				`/recovery.conf; rm -f ` + e.Spec.DataPath + `/recovery.done; `}
		} else {
			command = []string{`echo "` + mode + `" > /tmp/recovery.conf; echo "recovery_target_timeline='latest'" >> /tmp/recovery.conf;` +
				`echo "primary_conninfo='` + primaryConnInfo + `'" >> /tmp/recovery.conf; echo "primary_slot_name='` +
				primarySlotName + `'" >> /tmp/recovery.conf; sudo mv -f /tmp/recovery.conf ` + e.Spec.DataPath +
				`;sudo chown ` + e.Spec.EngineUser + ` ` + e.Spec.DataPath +
				`/recovery.conf; sudo rm -f ` + e.Spec.DataPath + `/recovery.done; `}
		}
	} else {
		command = []string{"sh", "-c", `echo "primary_slot_name='` + primarySlotName + `'" > /data/recovery.conf; echo "` + mode + `" >> /data/recovery.conf; echo "recovery_target_timeline='latest'" >> /data/recovery.conf;` +
			`echo "primary_conninfo='` + primaryConnInfo + `'" >> /data/recovery.conf; chown postgres /data/recovery.conf; rm -f /data/recovery.done; `}
	}

	stdout, stderr, err := e.Executor.Exec(command, 3, 20*time.Second)
	if err != nil {
		return errors.Wrapf(err, "Failed to exec %s out %s %s", command, stdout, stderr)
	}

	if false {
		command = []string{"bash", "-c", "su - postgres -c \"/u01/polardb_pg/bin/pg_ctl -D /data -W reload\""}
		stdout, stderr, err := e.Executor.Exec(command, 3, 20*time.Second)
		if err != nil {
			log.Warnf("Failed to demote %v err %s/%s/%s ", e, err.Error(), stdout, stderr)
		}
	} else {
		if force {
			if err = e.Stop(ImmediateStop, 0); err != nil {
				log.Warnf("Failed to immediate stop, err=%s, skip it", err.Error())
			}
			if err = e.Start(0); err != nil {
				log.Warnf("Failed to start, err=%s, skip it", err.Error())
			}
		} else {
			if err := e.Stop(FastStop, 1); err != nil {
				log.Warnf("Failed to fast stop %s err=%s, try immediate stop since it's RO", e.Spec.String(), err.Error())
				if err = e.Stop(ImmediateStop, 2); err != nil {
					log.Warnf("Failed to immediate stop, err=%s, skip it", err.Error())
				}
			}
			if err = e.Start(2); err != nil {
				log.Warnf("Failed to start, err=%s, skip it", err.Error())
			}
		}
	}

	if engineType == common.StandbyEngine && e.Spec.Sync == common.SYNC && e.Spec.ClusterType == common.SharedNothingCluster {
		rw := GetResourceManager().GetEngineManager(newRw.Endpoint)
		err = rw.SetSyncMode(&e.Spec, e.Spec.Sync)
		if err != nil {
			return errors.Wrapf(err, "Failed to sync %s to %s", e.Spec.String(), common.SYNC)
		}
	}

	return nil
}

func (e *PolarDBEngine) Stop(stopType EngineStopType, times int) error {
	var command []string
	retryTimes := 0
	start := time.Now()
	for {
		if e.Spec.IsRPMDeploy() {
			hasSudo := ""
			if *common.WorkUser != "root" {
				hasSudo = "sudo "
			}
			if stopType == FastStop {
				command = []string{hasSudo + "su - " + e.Spec.EngineUser + " -c \"/usr/local/polardb_o_current/bin/pg_ctl -D " + e.Spec.DataPath + " stop -m f\""}
			} else if stopType == ImmediateStop {
				command = []string{hasSudo + "su - " + e.Spec.EngineUser + " -c \"/usr/local/polardb_o_current/bin/pg_ctl -D " + e.Spec.DataPath + " stop -m i\""}
			} else {
				command = []string{hasSudo + "su - " + e.Spec.EngineUser + " -c \"/usr/local/polardb_o_current/bin/pg_ctl -D " + e.Spec.DataPath + " stop -m s\""}
			}
		} else {
			command = []string{"bash", "-c", "srv_opr_action=lock_stop_instance shutdown_mode=" + string(stopType) + " python /docker_script/entry_point.py"}
		}

		stdout, stderr, err := e.Executor.Exec(command, 0, 60*time.Second)
		if err != nil {
			log.Warn(e, "exec return ", err, stdout, stderr)
			if strings.Contains(stdout, "Is server running") {
				return nil
			}
			if retryTimes < times {
				retryTimes++
				if strings.Contains(err.Error(), "not exist") {
					sleepSec := 60 - time.Since(start).Seconds()
					if sleepSec > 0 {
						time.Sleep(time.Duration(sleepSec) * time.Second)
					} else {
						time.Sleep(time.Duration(retryTimes) * time.Second)
					}
				} else {
					time.Sleep(time.Duration(retryTimes) * time.Second)
				}
			} else {
				return errors.Wrapf(err, "Failed to exec %v", command)
			}
		} else {
			return nil
		}
	}
}

func (e *PolarDBEngine) Start(times int) error {
	var command []string
	retryTimes := 0
	start := time.Now()
	for {
		if e.Spec.IsRPMDeploy() {
			hasSudo := ""
			if *common.WorkUser != "root" {
				hasSudo = "sudo "
			}
			command = []string{hasSudo + "su - " + e.Spec.EngineUser + " -c \"/usr/local/polardb_o_current/bin/pg_ctl -D " +
				e.Spec.DataPath + " start -l " + e.Spec.DataPath + "/start.log\""}
		} else {
			command = []string{"bash", "-c", "srv_opr_action=unlock_start_instance python /docker_script/entry_point.py"}
		}

		stdout, stderr, err := e.Executor.Exec(command, 0, 60*time.Second)
		if err != nil {
			log.Warn(e, "exec return ", err, stdout, stderr)
			if retryTimes < times {
				if strings.Contains(err.Error(), "not exist") {
					sleepSec := 60 - time.Since(start).Seconds()
					if sleepSec > 0 {
						time.Sleep(time.Duration(sleepSec) * time.Second)
					} else {
						time.Sleep(time.Duration(retryTimes) * time.Second)
					}
				} else {
					time.Sleep(time.Duration(retryTimes) * time.Second)
				}
				retryTimes++
			} else {
				return errors.Wrapf(err, "Failed to exec %v", command)
			}
		} else {
			return nil
		}
	}

}

func (e *PolarDBEngine) ExecSQL(sql string, timeout time.Duration) (string, string, error) {
	command := []string{"bash", "-c", "psql -Upostgres -h /data -p 5432 -q -c '" + sql + "'"}
	return ExecInPod(command, EngineContainerName, e.Spec.PodName, e.NamespaceName, timeout)
}

func (e *PolarDBEngine) SetSyncMode(standby *common.InsSpec, mode common.SyncType) error {

	type SyncName struct {
		SynchronousStandbyNames string
	}

	var names SyncName
	times := 3
	for {
		db := GetResourceManager().GetMetricsConn(e.Spec.Endpoint)
		_, err := db.Query(&names, common.ShowSynchronousStandbyNames)
		if err != nil {
			if times == 0 {
				return errors.Wrapf(err, "Failed to query settings %s", common.ShowSynchronousStandbyNames)
			} else {
				log.Warnf("Failed to query settings %s err %s", common.ShowSynchronousStandbyNames, err.Error())
				GetResourceManager().ResetMetricsConn(e.Spec.Endpoint)
				times--
				continue
			}
		}
		break
	}
	standbyName := GenerateSlotName(common.StandbyEngine, standby)
	if mode == common.SYNC {
		if strings.Contains(names.SynchronousStandbyNames, standbyName) {
			log.Infof("synchronous_standby_names %s has %s", names.SynchronousStandbyNames, standbyName)
		} else {
			if names.SynchronousStandbyNames == "" {
				return e.AlterSQL(common.InternalMarkSQL+"ALTER SYSTEM set synchronous_standby_names = "+standbyName, time.Second*5)
			} else {
				return e.AlterSQL(common.InternalMarkSQL+"ALTER SYSTEM set synchronous_standby_names = "+names.SynchronousStandbyNames+","+standbyName, time.Second*5)
			}
		}
	} else {
		if strings.Contains(names.SynchronousStandbyNames, standbyName) {
			newName := ""
			for _, name := range strings.Split(names.SynchronousStandbyNames, ",") {
				if name != standbyName {
					newName += ","
					newName += name
				}
			}
			if newName == "" {
				return e.AlterSQL(common.InternalMarkSQL+"ALTER SYSTEM reset synchronous_standby_names", time.Second*5)
			} else {
				newName = newName[1:]
				return e.AlterSQL(common.InternalMarkSQL+"ALTER SYSTEM set synchronous_standby_names = "+newName, time.Second*5)
			}
		} else {
			log.Infof("synchronous_standby_names %s doesn't has %s", names.SynchronousStandbyNames, standbyName)
		}

	}
	return nil
}

func (e *PolarDBEngine) UpdateConf(settings []common.FileSetting) error {
	for _, setting := range settings {
		var err error
		if setting.Reset {
			err = e.AlterSQL(common.InternalMarkSQL+"ALTER SYSTEM reset "+setting.Name, time.Second*5)
		} else {
			err = e.AlterSQL(common.InternalMarkSQL+"ALTER SYSTEM set "+setting.Name+" = '"+setting.FileSetting+"'", time.Second*5)
		}
		if err != nil {
			return errors.Wrapf(err, "Failed to update conf %v on %s", setting, e.Spec.String())
		}
	}

	return nil
}

func (e *PolarDBEngine) AlterSQL(s string, timeout time.Duration) error {
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable connect_timeout=%d",
		e.Spec.Endpoint.Host, e.Spec.Endpoint.Port, GetClusterManagerConfig().Account.AuroraUser,
		GetClusterManagerConfig().Account.AuroraPassword, "postgres", int(timeout.Seconds()))
	db, err := sql.Open("postgres", conn)
	if err != nil {
		return errors.Wrapf(err, "Failed to alter %s sql %s", conn, s)
	}
	defer db.Close()
	st, err := db.Prepare(s)
	if err != nil {
		return errors.Wrapf(err, "Failed to alter %s sql %s", e.Spec.String(), s)
	}
	defer st.Close()
	_, err = st.Exec()
	if err != nil {
		return errors.Wrapf(err, "Failed to alter %s sql %s", e.Spec.String(), s)
	}
	_, err = db.Exec(common.ReloadConfSQL)
	if err != nil {
		return errors.Wrapf(err, "Failed to alter %s sql %s", e.Spec.String(), common.ReloadConfSQL)
	}
	log.Infof("Success to alter %s sql %s", e.Spec.String(), s)
	return nil
}

func (e *PolarDBEngine) LockReadonly(times int) error {
	if IsPolarPureMode() && e.Spec.IsRPMDeploy() {
		return e.AlterSQL(common.LockReadOnlySQL, time.Second*5)
	}

	var lockCmd string
	if IsPolarPureMode() {
		if GetResourceManager().GetOperatorClient().GetType() == "cloud" {
			lockCmd = `srv_opr_action=lock_ins_diskfull srv_opr_type=lock_ins srv_opr_hosts="[{\"access_port\":[` +
				e.Spec.Endpoint.Port + `]}]" srv_opr_host_ip="{}" python /docker_script/entry_point.py`
		} else {
			lockCmd = `srv_opr_type=node srv_opr_action=lock-ins-diskfull port=` + e.Spec.Endpoint.Port + ` /docker_script/entry_point_cm.py`
		}
	} else if IsPolarBoxMode() {
		lockCmd = `srv_opr_action=lock_ins_diskfull srv_opr_type=lock_ins srv_opr_hosts="[{\"access_port\":[` +
			e.Spec.Endpoint.Port + `]}]" srv_opr_host_ip="{}" python /docker_script/entry_point.py`
	} else {
		return errors.Errorf("Unsupport lock in %s mode", GetClusterManagerConfig().Mode)
	}

	retryTimes := 0
	for {
		command := []string{"bash", "-c", lockCmd}
		stdout, stderr, err := e.Executor.Exec(command, 0, 60*time.Second)
		if err != nil {
			log.Warn(e, "exec return ", err, stdout, stderr)
			if retryTimes < 3 {
				retryTimes++
				time.Sleep(time.Duration(retryTimes) * time.Second)
			} else {
				return errors.Wrapf(err, "Failed to exec %s", lockCmd)
			}
		} else {
			return nil
		}
	}

}

func (e *PolarDBEngine) UnlockReadonly(times int) error {
	if IsPolarPureMode() && e.Spec.IsRPMDeploy() {
		return e.AlterSQL(common.UnlockReadOnlySQL, time.Second*5)
	}

	var lockCmd string
	if IsPolarPureMode() {
		if GetResourceManager().GetOperatorClient().GetType() == "cloud" {
			lockCmd = `srv_opr_action=unlock_ins_diskfull srv_opr_type=lock_ins srv_opr_hosts="[{\"access_port\":[` +
				e.Spec.Endpoint.Port + `]}]" srv_opr_host_ip="{}" python /docker_script/entry_point.py`
		} else {
			lockCmd = `srv_opr_type=node srv_opr_action=unlock-ins-diskfull port=` + e.Spec.Endpoint.Port + ` /docker_script/entry_point_cm.py`
		}
	} else if IsPolarBoxMode() {
		lockCmd = `srv_opr_action=unlock_ins_diskfull srv_opr_type=lock_ins srv_opr_hosts="[{\"access_port\":[` +
			e.Spec.Endpoint.Port + `]}]" srv_opr_host_ip="{}" python /docker_script/entry_point.py`
	} else {
		return errors.Errorf("Unsupport unlock in %s mode", GetClusterManagerConfig().Mode)
	}

	retryTimes := 0
	for {
		command := []string{"bash", "-c", lockCmd}
		stdout, stderr, err := e.Executor.Exec(command, times, 60*time.Second)
		if err != nil {
			log.Warn(e, "exec return ", err, stdout, stderr)
			if retryTimes < 3 {
				retryTimes++
				time.Sleep(time.Duration(retryTimes) * time.Second)
			} else {
				return errors.Wrapf(err, "Failed to exec %s", lockCmd)
			}
		} else {
			return nil
		}
	}

}
