package action

import (
	"context"
	"fmt"
	"github.com/go-pg/pg"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"time"
)

type LeaderFollowerSwitchActionExecutor struct {
	SwitchActionExecutor
}

func (e *LeaderFollowerSwitchActionExecutor) String() string {
	return "LeaderFollowerSwitchActionExecutor: " + e.SwitchAction.String()
}

func (a *LeaderFollowerSwitchActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	startTs := time.Now()
	log.Infof("Start Switch from %s to %s", a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String())

	err := ExecuteWithRetry(a.StartSwitchTask, 3, false)
	if err != nil {
		return err
	}
	prePhase := metaManager.GetPhase()
	metaManager.SwitchPhase(common.SwitchingPhase)
	if metaManager.ClusterMeta.MasterClusterID == a.SwitchAction.NewRw.ClusterID {
		log.Infof("Success Switch %s already master", a.SwitchAction.NewRw.ClusterID)
	} else if metaManager.ClusterMeta.MasterClusterID != a.SwitchAction.OldRw.ClusterID {
		log.Infof("Skip Switch old master %s already change to %s", a.SwitchAction.OldRw.ClusterID, metaManager.GetMasterClusterID())
	} else {
		a.DisableSwitchInsStatus(metaManager, statusManager)
		err = a.ExecuteSwitch(metaManager, statusManager)
		a.EnableSwitchInsStatus(metaManager, statusManager)
		ExecuteWithRetry(metaManager.Sync, 10, true)
	}
	if prePhase == common.SwitchingPhase {
		metaManager.SwitchPhase(common.RunningPhase)
	} else {
		metaManager.SwitchPhase(prePhase)
	}
	ExecuteWithRetry(a.FinishSwitchTask, 3, true)

	if err != nil {
		log.Infof("Failed Switch from %s to %s err %s decision %s cost %s",
			a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String(), err.Error(), a.Event.Reason.DecisionTime, a.Event.SwitchTime.String())
		return err
	} else {
		log.Infof("Success Switch from %s to %s decision %s rto %s cost %s",
			a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String(), a.Event.Reason.DecisionTime, a.Rto.String(), time.Since(startTs).String())
		return nil
	}

	return nil
}

func (a *LeaderFollowerSwitchActionExecutor) LeadershipTransferToServer() error {
	startTs := time.Now()
	rw := resource.GetResourceManager().GetMetricsConn(a.SwitchAction.OldRw.Endpoint)
	sql := `alter system dma change leader to '` + a.SwitchAction.NewRw.Endpoint.String() + `'`
	_, err := rw.Exec(sql)

	l := fmt.Sprintf("transfer leader from %s to %s with sql %s",
		a.SwitchAction.OldRw.String(), a.SwitchAction.NewRw.String(), sql)
	a.LogSwitchEvent("LeadershipTransferToServer", startTs, l, &err)

	return err
}

func (a *LeaderFollowerSwitchActionExecutor) FollowerRequestVote() error {
	startTs := time.Now()
	sql := `alter system dma force change leader;`
	var l string
	var err error

	if a.CheckPaxosLeader(&a.SwitchAction.NewRw) {
		l = fmt.Sprintf("%s already paxos leader skip request vote", a.SwitchAction.NewRw.String())
		return nil
	}

	rw := resource.GetResourceManager().GetMetricsConn(a.SwitchAction.NewRw.Endpoint)
	_, err = rw.Exec(sql)
	if err != nil {
		log.Warnf("Failed to %s for %s err %s", sql, a.SwitchAction.NewRw.String(), err.Error())
	}
	var n []string
	n = append(n, a.SwitchAction.NewRw.String())

	time.Sleep(2 * time.Second)

	if a.CheckPaxosLeader(&a.SwitchAction.NewRw) {
		l = fmt.Sprintf("%s already paxos leader skip request vote other standby", a.SwitchAction.NewRw.String())
		return nil
	}

	for _, standby := range a.SwitchAction.OldStandby {
		if standby.SwitchPriority != -1 {
			c := resource.GetResourceManager().GetMetricsConn(standby.Endpoint)
			_, err := c.Exec(sql)
			if err != nil {
				log.Warnf("Failed to %s for %s err %s", sql, a.SwitchAction.NewRw.String(), err.Error())
			}
			n = append(n, standby.String())
		}
	}

	l = fmt.Sprintf("force %v change leader with sql %s", n, sql)

	defer func() {
		a.LogSwitchEvent("FollowerRequestVote", startTs, l, &err)
	}()

	return err
}

func (a *LeaderFollowerSwitchActionExecutor) CheckPaxosLeader(spec *common.InsSpec) bool {
	db := resource.GetResourceManager().GetEngineConn(spec.Endpoint)

	type PaxosStatus struct {
		CommitIndex int64
		PaxosRole   int
		tableName   struct{} `pg:",discard_unknown_columns"`
	}
	var s PaxosStatus

	_, err := db.QueryOne(&s, common.PaxosStatusSQL)
	if err == nil {
		if s.PaxosRole == 2 {
			return true
		}
	} else {
		resource.GetResourceManager().ResetEngineConn(spec.Endpoint)
	}

	return false
}

func (a *LeaderFollowerSwitchActionExecutor) DetectNewLeader(spec *common.InsSpec, ctx context.Context, res chan *common.InsSpec) {
	defer log.Infof("DetectNewLeader %s quit!", spec.String())
	c := 0
	waitTime := a.SwitchAction.RTO - 10
	if waitTime < 50 {
		waitTime = 50
	}
	paxosLeader := false
	start := time.Now()
	for {
		c++
		db := resource.GetResourceManager().GetEngineConn(spec.Endpoint)
		var ts int64
		_, err := db.Query(pg.Scan(&ts), common.PolarDBRwHealthCheckSQL)
		if err != nil {
			if c%10 == 0 {
				log.Warnf("%s still not leader with err %s cost %s", spec.String(), err.Error(), time.Since(start).String())

				if a.CheckPaxosLeader(spec) {
					paxosLeader = true
					log.Infof("%s already paxos leader but still not ready cost %s", spec.String(), time.Since(start).String())
				}
			}
			resource.GetResourceManager().ResetEngineConn(spec.Endpoint)
			select {
			case <-time.After(time.Millisecond * 200):
				if paxosLeader && int(time.Since(start).Seconds()) > waitTime {
					log.Infof("%s already paxos leader for %s, select it for leader", spec.String(), time.Since(start).String())
					break
				}
				continue
			case <-ctx.Done():
				// paxos already has new leader, but not ready to write before timeout
				if paxosLeader {
					err = nil
				} else {
					return
				}
			}
		}

		res <- spec
		return
	}
}

func (a *LeaderFollowerSwitchActionExecutor) WaitNewLeaderReady() error {
	startTs := time.Now()
	var err error = nil
	newLeaderChan := make(chan *common.InsSpec)

	time.Sleep(time.Second)

	ctx, cancel := context.WithCancel(context.Background())

	go a.DetectNewLeader(&a.SwitchAction.NewRw, ctx, newLeaderChan)
	go a.DetectNewLeader(&a.SwitchAction.OldRw, ctx, newLeaderChan)
	for _, ins := range a.SwitchAction.OldStandby {
		go a.DetectNewLeader(&ins, ctx, newLeaderChan)
	}

	waitTime := a.SwitchAction.RTO
	if waitTime < 60 {
		waitTime = 60
	}

	var newLeader *common.InsSpec
	select {
	case newLeader = <-newLeaderChan:
		cancel()
		if newLeader.Endpoint != a.SwitchAction.NewRw.Endpoint {
			log.Infof("New Leader Expect %s Actual %s", a.SwitchAction.NewRw.String(), newLeader.String())
			a.SwitchAction.OldStandby = append(a.SwitchAction.OldStandby, a.SwitchAction.NewRw)
			var tmpOldStandby []common.InsSpec
			for _, ins := range a.SwitchAction.OldStandby {
				if ins.Endpoint != newLeader.Endpoint {
					tmpOldStandby = append(tmpOldStandby, ins)
				}
			}
			tmpOldStandby = append(tmpOldStandby, a.SwitchAction.NewRw)
			a.SwitchAction.OldStandby = tmpOldStandby
			a.SwitchAction.NewRw = *newLeader
		}
		break
	case <-time.After(time.Duration(waitTime) * time.Second):
		cancel()
		err = errors.Errorf("Wait New Leader Timeout")
	}

	l := fmt.Sprintf("wait leader %s ready", a.SwitchAction.NewRw.String())
	a.LogSwitchEvent("WaitLeaderReady", startTs, l, &err)

	return err
}

func (a *LeaderFollowerSwitchActionExecutor) ExecuteSwitch(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	var err error

	if a.SwitchAction.Manual {
		err = a.LeadershipTransferToServer()
		if err != nil {
			return err
		}
	} else {
		err = ExecuteWithRetry(a.FollowerRequestVote, 3, false)
		if err != nil {
			log.Warnf("Failed to force %s request vote err %s", a.SwitchAction.NewRw.String(), err.Error())
		}
	}

	err = a.WaitNewLeaderReady()
	if err != nil {
		log.Warnf("Failed to wait new leader err=%s", err.Error())
		return err
	}

	err = ExecuteWithRetry(a.SwitchRwVip, 3, false)
	if err != nil {
		// rollback, ignore error cause vip check will recover it
		ExecuteWithRetry(a.RollbackRwVip, 3, false)
		log.Warnf("Failed to SwitchRwVip, err=%s", err.Error())
		return err
	}

	a.Rto = time.Since(a.Event.Reason.RtoStart)
	a.Event.RTO = a.Rto

	ExecuteWithRetry(a.UpdateK8sStatus, 3, false)

	metaManager.ClusterMeta.MasterClusterID = a.SwitchAction.NewRw.ClusterID

	return nil
}
