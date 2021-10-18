package resource

import (
	"encoding/json"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"sync"
)

type ConfigManager struct {
	ConfigMutex      sync.Mutex
	sqlMonitorConfig common.SQLMonitorConfig
}

var configManagerOnce sync.Once
var configManagerIns *ConfigManager

func GetConfigManager() *ConfigManager {
	configManagerOnce.Do(func() {
		configManagerIns = &ConfigManager{}
	})
	return configManagerIns
}

func (m *ConfigManager) UpdateSQLMonitorConfig(buf string) error {
	var c common.SQLMonitorConfig
	err := json.Unmarshal([]byte(buf), &c)
	if err != nil {
		return errors.Wrapf(err, "Failed to unmarshal SQLMonitorConfig buf %s", buf)
	}
	log.Infof("Success to marshal SQLMonitorConfig %v", c)
	m.ConfigMutex.Lock()
	defer m.ConfigMutex.Unlock()

	m.sqlMonitorConfig = c

	return nil
}

func (m *ConfigManager) GetSQLMonitorConfig() *common.SQLMonitorConfig {
	c := &common.SQLMonitorConfig{}
	m.ConfigMutex.Lock()
	defer m.ConfigMutex.Unlock()

	for _, item := range m.sqlMonitorConfig.Items {
		c.Items = append(c.Items, item)
	}

	return c
}
