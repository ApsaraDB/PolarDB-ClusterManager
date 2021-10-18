package resource

import (
	"encoding/json"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"io/ioutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"os/user"
	"strings"
	"sync"
)

type ClusterManagerConfig struct {
	Cluster                 ClusterInfo          `json:"cluster_info"`
	Account                 AccountInfo          `json:"account_info"`
	Consensus               ConsensusInfo        `json:"consensus"`
	NodeDriver              NodeDriverInfo       `json:"node_driver_info"`
	UE                      UniverseExplorerInfo `json:"ue"`
	BackupAgent             BackupAgentInfo      `json:"backup_agent"`
	Mode                    common.WorkMode      `json:"work_mode"`
	OperatorLabel           string               `json:"operator_label"`
	ClientInterface         string               `json:"client_interface"`
	OperatorNamespace       string               `json:"operator_namespace"`
	ManagerServiceName      string               `json:"manager_service_name"`
	ManagerServiceNamespace string               `json:"manager_service_namespace"`
	DBInstanceName          string               `json:"db_instance_name"`
	StandbyRPO              int                  `json:"standby_rpo"`
	WorkUser                string               `json:"work_user"`
	SystemIdentify          string               `json:"system_identify"`
	DefaultHaPolicy         string               `json:"default_ha_policy"`
}

type BackupAgentInfo struct {
	Port int `json:"port"`
}

type UniverseExplorerInfo struct {
	Port        int `json:"port"`
	MetricsPort int `json:"metrics_port"`
}

type NodeDriverInfo struct {
	Port int `json:"port"`
}

type ConsensusInfo struct {
	Port        int  `json:"port"`
	Recovery    bool `json:"recovery,omitempty"`
	EtcdStorage bool `json:"etcd_storage,omitempty"`
}

type AccountInfo struct {
	AuroraUser      string `json:"aurora_user"`
	AuroraPassword  string `json:"aurora_password"`
	ReplicaUser     string `json:"replica_user"`
	ReplicaPassword string `json:"replica_password"`
}

type ClusterInfo struct {
	Namespace       string `json:"namespace"`
	Name            string `json:"name"`
	LogicID         string `json:"logic_id"`
	Port            int    `json:"port"`
	PrimaryService  string `json:"primary_service_name"`
	DiskQuota       int    `json:"disk_quota"`
	SmartClientPort int    `json:"smartclient_port"`
}

type SlbInfo struct {
	SlbEndpoint string `json:"slb_endpoint"`
	Bid         string `json:"bid"`
	AccessID    string `json:"access_id"`
	AccessKey   string `json:"access_key"`
	RegionNo    string `json:"region_no"`
	UserId      string `json:"user_id"`
	SlbID       string `json:"slb_id"`
	FrontPort   string `json:"front_port"`
}

var clusterManagerConfigOnce sync.Once
var clusterManagerConfig *ClusterManagerConfig

func GetClusterManagerConfig() *ClusterManagerConfig {
	clusterManagerConfigOnce.Do(func() {
		clusterManagerConfig = &ClusterManagerConfig{}
	})
	return clusterManagerConfig
}

func (c *ClusterManagerConfig) Initialize() (err error) {

	c.CheckWorkMode()

	if IsPolarBoxMode() || IsPolarCloudMode() {
		if err = c.LoadConfigFromEnv(); err == nil {
			return c.CheckConfig()
		}
	} else {
		if err = c.LoadConfigFromFile(); err == nil {
			return c.CheckConfig()
		}
	}

	return err
}

func (c *ClusterManagerConfig) CheckWorkMode() {
	workMode := os.Getenv("work_mode")
	if workMode != "" {
		c.Mode = common.WorkMode(workMode)
		log.Infof("ClusterManager work in %s mode from env", c.Mode)
	}
}

func (c *ClusterManagerConfig) CheckConfig() error {
	if c.Mode == common.POLAR_PAAS_MS || c.Mode == common.POLAR_PURE {
		if c.Consensus.Port == 0 {
			return errors.Errorf("Invalid parameter %v", c)
		}
		*common.V1Compatibility = false

		if operatorConf.Type == "mpd" || operatorConf.Type == "cloud" {
			*common.AutoStandbyFailover = false
			*common.AutoReplicaFailover = true
		} else {
			*common.AutoStandbyFailover = true
			*common.AutoReplicaFailover = false
		}

		if c.StandbyRPO != 0 {
			*common.StandbyRPO = c.StandbyRPO
		}

		if *common.LogFile != "" {
			logPath := *common.WorkDir + "/log"
			err := os.MkdirAll(logPath, 0755)
			if err != nil {
				log.Warnf("Failed to make log dir %s err %s", logPath, err.Error())
			}
			log.SetOutputByName(logPath + "/" + *common.LogFile)
			log.SetRotateByDay()
			log.SetHighlighting(false)
		}

		if c.WorkUser != "" {
			*common.WorkUser = c.WorkUser
		} else {
			u, err := user.Current()
			if err != nil {
				return errors.Wrapf(err, "Failed to get current user")
			}
			*common.WorkUser = u.Username
		}
		log.Infof("ClusterManager work user %s", *common.WorkUser)
	}

	if c.BackupAgent.Port == 0 {
		c.BackupAgent.Port = 817
	}
	if c.UE.MetricsPort == 0 {
		c.UE.MetricsPort = 818
	}
	if c.UE.Port == 0 {
		c.UE.Port = 819
	}

	return nil
}

func (c *ClusterManagerConfig) LoadConfigFromFile() error {

	path := *common.WorkDir + "/" + *common.ConfigFile
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Warnf("Failed to load config json from %s err %s", path, err.Error())
		return nil
	}

	err = json.Unmarshal(data, c)
	if err != nil {
		log.Warnf("Failed to parse config json %s err %s", string(data), err.Error())
		return err
	}

	log.Infof("Success to load conf from %s work in %s mode", path, c.Mode)
	return nil
}

func (c *ClusterManagerConfig) LoadConfigFromEnv() error {

	logFileName := os.Getenv("CM_LOG_FILE_NAME")
	logRotateByDay := os.Getenv("CM_LOG_FILE_ROTATE_BY_DAY")

	logFileName = strings.TrimSpace(logFileName)

	logRotateByDay = strings.ToUpper(strings.TrimSpace(logRotateByDay))

	bLogRotateByDay := true

	if logRotateByDay == "NO" || logRotateByDay == "FALSE" || logRotateByDay == "0" || logRotateByDay == "N" {
		bLogRotateByDay = false
	}

	if logFileName != "" {
		err := log.SetOutputByName(logFileName)
		if err != nil {
			log.Errorf("set log file name err: %v", err)
		} else {
			if bLogRotateByDay {
				log.SetRotateByDay()
			} else {
				log.SetRotateByHour()
			}
		}
		log.SetHighlighting(false)
	}

	clusterInfo := os.Getenv("cluster_info")
	if err := json.Unmarshal([]byte(clusterInfo), &c.Cluster); err != nil {
		log.Fatal("Failed to Parse polardb cluster info env", clusterInfo, err)
		return err
	}

	accountInfo := os.Getenv("account_info")
	if err := json.Unmarshal([]byte(accountInfo), &c.Account); err != nil {
		log.Warn("Failed to Parse polardb account info from env", accountInfo, err)
		client, _, err := GetDefaultKubeClient()
		if err != nil {
			log.Fatal("Failed to Parse polardb account info env", err)
			return err
		}
		config, err := client.CoreV1().ConfigMaps(c.Cluster.Namespace).Get(c.Cluster.Name+".cm", v1.GetOptions{})
		if err != nil {
			log.Fatal("Failed to Parse polardb account info env", err)
			return err
		}
		accountInfo = config.Data["account_info"]
		if err := json.Unmarshal([]byte(accountInfo), &c.Account); err != nil {
			log.Fatal("Failed to Parse polardb account info env", accountInfo, err)
			return err
		}
	}
	clientInterface := os.Getenv("client_interface_name")
	if clientInterface != "" {
		c.ClientInterface = clientInterface
	}

	operatorLabel := os.Getenv("OPERATOR_LABEL")
	if operatorLabel != "" {
		c.OperatorLabel = operatorLabel
	} else {
		log.Warnf("Failed to get operator label")
	}

	operatorNamespace := os.Getenv("OPERATOR_NAMESPACE")
	if operatorNamespace != "" {
		c.OperatorNamespace = operatorNamespace
	} else {
		log.Warnf("Failed to get operator namespace")
	}

	return nil
}

func IsPolarSharedMode() bool {
	if GetClusterManagerConfig().Mode == common.POLAR_BOX_MUL || GetClusterManagerConfig().Mode == common.POLAR_CLOUD {
		return true
	} else {
		return false
	}
}

func IsPolarBoxMode() bool {
	if GetClusterManagerConfig().Mode == common.POLAR_BOX_ONE || GetClusterManagerConfig().Mode == common.POLAR_BOX_MUL {
		return true
	} else {
		return false
	}
}

func IsPolarCloudMode() bool {
	if GetClusterManagerConfig().Mode == common.POLAR_CLOUD {
		return true
	} else {
		return false
	}
}

func IsPolarBoxOneMode() bool {
	if GetClusterManagerConfig().Mode == common.POLAR_BOX_ONE {
		return true
	} else {
		return false
	}
}

func IsPolarBoxMulMode() bool {
	if GetClusterManagerConfig().Mode == common.POLAR_BOX_MUL {
		return true
	} else {
		return false
	}
}

func IsPolarPaasMode() bool {
	if GetClusterManagerConfig().Mode == common.POLAR_PAAS_MS {
		return true
	} else {
		return false
	}
}

func IsPolarPureMode() bool {
	if GetClusterManagerConfig().Mode == common.POLAR_PURE {
		return true
	} else {
		return false
	}
}

func IsEtcdStorage() bool {
	return GetClusterManagerConfig().Consensus.EtcdStorage
}

func IsCloudOperator() bool {
	return operatorConf.Type == "cloud"
}

func IsMpdOperator() bool {
	return operatorConf.Type == "mpd"
}
