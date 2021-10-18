package meta

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"k8s.io/client-go/tools/leaderelection"
)

const (
	ClusterManagerConfigKey    = "ClusterManagerConfig"
	ClusterManagerAlarmKey     = "ClusterManagerAlarm"
	ClusterManagerSwitchLogKey = "ClusterManagerSwitchLog"
)

type K8sConsensusService struct {
}

func NewK8sConsensusService() *K8sConsensusService {
	return &K8sConsensusService{}
}

func (s *K8sConsensusService) LeaderTransfer(nodeID, addr string) error {
	return nil
}

func (s *K8sConsensusService) MemberStatus() map[string]string {
	return map[string]string{}
}

func (s *K8sConsensusService) Status() string {
	return ""
}

func (s *K8sConsensusService) Get(key string) ([]byte, error) {
	if key == ClusterManagerConfigKey {
		c, err := resource.GetResourceManager().GetK8sClient().GetConfigMap()
		if err != nil {
			return nil, err
		}
		return json.Marshal(c)
	} else {
		return nil, errors.Errorf("key %s not exist!", key)
	}
}

func (s *K8sConsensusService) Set(key string, value []byte) error {
	if key == ClusterManagerConfigKey {
		var cmConfig common.ClusterManagerConfig
		err := json.Unmarshal(value, &cmConfig)
		if err != nil {
			return errors.Wrapf(err, "value %v is not ClusterManagerConfig!", value)
		}
		return resource.GetResourceManager().GetK8sClient().UpdateConfigMap(&cmConfig)
	} else if key == ClusterManagerAlarmKey {
		var alarms []common.AlarmInfo
		err := json.Unmarshal(value, &alarms)
		if err != nil {
			return errors.Wrapf(err, "value %v is not AlarmInfo!", value)
		}
		return resource.GetResourceManager().GetK8sClient().UpdateAlarmConfigMap(alarms)
	} else if key == ClusterManagerSwitchLogKey {
		var event common.SwitchEvent
		err := json.Unmarshal(value, &event)
		if err != nil {
			return errors.Wrapf(err, "Failed to unmarshal switch event %v", event)
		}

		return resource.GetResourceManager().GetK8sClient().AddSwitchEvent(&event)
	} else {
		return errors.Errorf("key %s not support!", key)
	}
}

func (s *K8sConsensusService) Delete(key string) error {
	return errors.New("Not Support")
}

func (s *K8sConsensusService) Run(enableRecovery bool, ctx context.Context, lec leaderelection.LeaderElectionConfig) error {
	leaderelection.RunOrDie(ctx, lec)
	return nil
}

func (s *K8sConsensusService) Join(nodeID, addr string) error {
	return nil
}

func (s *K8sConsensusService) Remove(nodeID, addr string) error {
	return nil
}
