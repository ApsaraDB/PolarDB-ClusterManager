package resource

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/polardb"
	"google.golang.org/grpc"
	"strconv"
	"strings"
	"time"
)

type NodeDriverManager struct {
	Spec *common.InsSpec
}

func (e *NodeDriverManager) UpdateConf(settings []common.FileSetting) error {
	panic("implement me")
}

func (e *NodeDriverManager) InsSpec() *common.InsSpec {
	return e.Spec
}

func (e *NodeDriverManager) RequestNodeDriver(req *polardb.NodeNotifyHAStateRequest) error {
	if e.Spec.PodName != "" {
		req.Namespace = GetClusterManagerConfig().Cluster.Namespace
		req.Member.PodName = e.Spec.PodName
	} else {
		req.ClusterId = e.Spec.NodeDriverClusterID
		req.Member.Id = e.Spec.NodeDriverMemberID
	}

	conn, err := grpc.Dial(e.Spec.Endpoint.Host+":"+strconv.Itoa(GetClusterManagerConfig().NodeDriver.Port), grpc.WithInsecure())
	if err != nil {
		log.Warnf("Failed to connect to node driver service err=%s", err.Error())
		return err
	}
	defer conn.Close()

	ctx, _ := context.WithTimeout(context.Background(), time.Second*60)
	c, err := polardb.NewClusterNodeControllerClient(conn).NodeNotifyHAState(ctx)
	if err != nil {
		log.Warnf("Failed to request to node driver service err=%s", err.Error())
		return err
	}
	defer c.CloseSend()

	if err = c.Send(req); err != nil {
		log.Warnf("Failed to send to node driver service err=%s", err.Error())
		return err
	}

	resp, err := c.Recv()
	if err != nil {
		log.Warnf("Failed to recv from node driver service err=%s", err.Error())
		return err
	}
	if resp.Status == polardb.NodeNotifyHAStateResponse_Succeed {
		return nil
	} else {
		return errors.Errorf("resp %s status not success", resp.String())
	}
}

func (e *NodeDriverManager) onlinePromote(engineType common.EngineType, newRos []common.InsSpec) error {
	log.Infof("Starting Online Promote %s to rw", e.Spec.String())

	context := map[string]string{
		"online": "1",
	}

	req := &polardb.NodeNotifyHAStateRequest{
		Member: &polardb.Member{
			Role:    polardb.Member_MASTER,
			Status:  polardb.Member_Running,
			Context: context,
		},
	}

	err := e.RequestNodeDriver(req)
	if err != nil {
		return errors.Wrapf(err, "Failed to req %v", *req)
	}

	return nil
}

func (e *NodeDriverManager) offlinePromote(engineType common.EngineType, newRos []common.InsSpec) error {
	log.Infof("Starting Offline Promote %s to rw", e.Spec.String())

	context := map[string]string{
		"online": "0",
	}

	req := &polardb.NodeNotifyHAStateRequest{
		Member: &polardb.Member{
			Role:    polardb.Member_MASTER,
			Status:  polardb.Member_Running,
			Context: context,
		},
	}

	err := e.RequestNodeDriver(req)
	if err != nil {
		return errors.Wrapf(err, "Failed to req %v", *req)
	}

	return nil
}

func (e *NodeDriverManager) Promote(engineType common.EngineType, newRos []common.InsSpec, online bool) error {
	if online {
		return e.onlinePromote(engineType, newRos)
	} else {
		return e.offlinePromote(engineType, newRos)
	}

	return nil
}

func (e *NodeDriverManager) Demote(engineType common.EngineType, newRw common.InsSpec, online bool, force bool) error {
	log.Infof("Starting Demote %s to %s", e.Spec.String(), engineType)

	var role polardb.Member_Role
	if engineType == common.RoEngine {
		role = polardb.Member_REPLICA
	} else if engineType == common.StandbyEngine {
		role = polardb.Member_STANDBY
	} else if engineType == common.DataMax {
		role = polardb.Member_DATAMAX
	} else {
		return errors.Errorf("Unknow engine type %s", engineType)
	}

	context := map[string]string{
		"rw_host":              newRw.Endpoint.Host,
		"rw_port":              newRw.Endpoint.Port,
		"replication_user":     GetClusterManagerConfig().Account.ReplicaUser,
		"replication_password": GetClusterManagerConfig().Account.ReplicaPassword,
		"replication_slot":     GenerateSlotName(engineType, e.Spec),
	}

	req := &polardb.NodeNotifyHAStateRequest{
		Member: &polardb.Member{
			Role:    role,
			Status:  polardb.Member_Running,
			Context: context,
		},
	}

	err := e.RequestNodeDriver(req)
	if err != nil {
		return errors.Wrapf(err, "Failed to req %v", *req)
	}

	if engineType == common.StandbyEngine && e.Spec.Sync == common.SYNC && e.Spec.ClusterType == common.SharedNothingCluster {
		rw := GetResourceManager().GetEngineManager(newRw.Endpoint)
		err = rw.SetSyncMode(e.Spec, e.Spec.Sync)
		if err != nil {
			return errors.Wrapf(err, "Failed to sync %s to %s", e.Spec.String(), common.SYNC)
		}
	}

	return nil
}

func (e *NodeDriverManager) Stop(stopType EngineStopType, times int) error {
	req := &polardb.NodeNotifyHAStateRequest{
		Member: &polardb.Member{
			Status: polardb.Member_Stopped,
		},
	}

	err := e.RequestNodeDriver(req)
	if err != nil {
		return errors.Wrapf(err, "Failed to req %v", *req)
	}

	return nil
}

func (e *NodeDriverManager) Start(times int) error {
	if e.Spec.ClusterType == common.PaxosCluster {
		return e.offlinePromote(common.RwEngine, nil)
	} else {
		return errors.New("Not Supported")
	}
}

func (e *NodeDriverManager) ExecSQL(sql string, timeout time.Duration) (string, string, error) {
	return "", "", errors.New("Not Supported")
}

func (e *NodeDriverManager) SetSyncMode(standby *common.InsSpec, mode common.SyncType) error {
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

func (e *NodeDriverManager) AlterSQL(s string, timeout time.Duration) error {
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

func (e *NodeDriverManager) LockReadonly(times int) error {
	req := &polardb.NodeNotifyHAStateRequest{
		Member: &polardb.Member{
			Status:     polardb.Member_Running,
			AccessMode: polardb.Member_ReadOnly,
		},
	}

	err := e.RequestNodeDriver(req)
	if err != nil {
		return errors.Wrapf(err, "Failed to req %v", *req)
	}

	return nil
}

func (e *NodeDriverManager) UnlockReadonly(times int) error {
	req := &polardb.NodeNotifyHAStateRequest{
		Member: &polardb.Member{
			Status:     polardb.Member_Running,
			AccessMode: polardb.Member_ReadWrite,
		},
	}

	err := e.RequestNodeDriver(req)
	if err != nil {
		return errors.Wrapf(err, "Failed to req %v", *req)
	}

	return nil
}
