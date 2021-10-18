package resource

import (
	"encoding/json"
	"github.com/go-pg/pg"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/smartclient_service"
	"io/ioutil"
	"sync"
	"time"
)

type ResourceManager struct {
	// clusterID -> store
	Store              map[string]StoreManager
	Engine             map[common.EndPoint]EngineManager
	Conn               map[common.EndPoint]*pg.DB
	MetricsConn        map[common.EndPoint]*pg.DB
	ConnMutex          sync.Mutex
	Client             *K8sClient
	VipM               VipManager
	SmartClientService *smartclient_service.SmartClientService
}

var once sync.Once
var ins *ResourceManager

func GetResourceManager() *ResourceManager {
	once.Do(func() {
		ins = &ResourceManager{
			Store:       make(map[string]StoreManager),
			Engine:      make(map[common.EndPoint]EngineManager),
			Conn:        make(map[common.EndPoint]*pg.DB),
			MetricsConn: make(map[common.EndPoint]*pg.DB),
		}
	})
	return ins
}

func (m *ResourceManager) Initialize() error {

	m.InitializeOperatorConf()

	// initialize cluster manager config
	// todo
	config := GetClusterManagerConfig()
	if err := config.Initialize(); err != nil {
		return err
	}

	m.Client = &K8sClient{
		NamespaceName: config.Cluster.Namespace,
		ClusterName:   config.Cluster.Name,
	}

	m.VipM = NewLinuxVipManager()

	return nil
}

func (m *ResourceManager) InitializeOperatorConf() {
	path := *common.WorkDir + "/conf/subscribe.conf"
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Warnf("Failed to load config json from %s err %s", path, err.Error())
		return
	}

	err = json.Unmarshal(data, &operatorConf)
	if err != nil {
		log.Fatalf("Failed to unmarshal config json %s err %s", string(data), err.Error())
	}
}

func (m *ResourceManager) Finalize() {
	m.ConnMutex.Lock()
	defer m.ConnMutex.Unlock()
	for k, c := range m.Conn {
		c.Close()
		delete(m.Conn, k)
	}
}

func (m *ResourceManager) UpdateInstanceResource(spec *common.ClusterSpecMeta) error {

	info := GetClusterManagerConfig()
	// initialize polar store & engine manager
	for _, subCluster := range spec.SubCluster {
		if _, exist := m.Store[subCluster.ClusterID]; !exist {
			// initialize polar store
			if IsPolarBoxMode() {
				pvcName := info.Cluster.Name + "-" + subCluster.ClusterID
				m.Store[subCluster.ClusterID] = NewSanStore(pvcName)
			} else if IsPolarPureMode() && !IsCloudOperator() {
				m.Store[subCluster.ClusterID] = &FakeStore{}
			} else if IsCloudOperator() || IsPolarCloudMode() {
				pvcName := info.Cluster.Name + "-" + subCluster.ClusterID
				var polarStore PolarStore
				err := m.Client.GetPolarStore(pvcName, &polarStore)
				if err != nil {
					log.Warnf("Failed to GetPolarStore err %s", err.Error())
				} else {
					m.Store[subCluster.ClusterID] = &polarStore
					log.Infof("Success to add store manager %s", pvcName)
				}
			}
		}
		for _, ins := range subCluster.InstanceSpec {
			if e, exist := m.Engine[ins.Endpoint]; !exist || e.InsSpec().PodName != ins.PodName {
				var executor CommandExecutor
				if ins.IsDockerDeploy() {
					executor = &K8sCommandExecutor{
						Namespace:     info.Cluster.Namespace,
						PodName:       ins.PodName,
						ContainerName: ManagerContainerName,
					}
				} else {
					executor = &SshCommandExecutor{
						Host: ins.Endpoint.Host,
					}
				}

				if ins.UseNodeDriver {
					m.Engine[ins.Endpoint] = &NodeDriverManager{
						Spec: ins,
					}
				} else {
					if ins.IsRPMDeploy() || ins.IsDockerDeploy() {
						m.Engine[ins.Endpoint] = &PolarDBEngine{
							Spec:            *ins,
							NamespaceName:   info.Cluster.Namespace,
							ReplicaUser:     info.Account.ReplicaUser,
							ReplicaPassword: info.Account.ReplicaPassword,
							Executor:        executor,
						}
					} else {
						return errors.Errorf("Unsupport deploy mode for ins %v", ins)
					}
				}
				log.Infof("Success to add engine manager %s", ins.String())
			}
		}
	}

	return nil
}

func (m *ResourceManager) GetVipManager() VipManager {
	return m.VipM
}

func (m *ResourceManager) GetStoreManager(clusterID string) StoreManager {
	return m.Store[clusterID]
}

func (m *ResourceManager) GetEngineManager(ep common.EndPoint) EngineManager {
	if m, exist := m.Engine[ep]; exist {
		return m
	} else {
		return nil
	}
}

func (m *ResourceManager) GetK8sClient() *K8sClient {
	return m.Client
}

var operatorOnce sync.Once
var operatorClient OperatorClientInterface

type OperatorConf struct {
	Type string `json:"type"`
}

var operatorConf OperatorConf

func (m *ResourceManager) GetOperatorClient() OperatorClientInterface {

	operatorOnce.Do(func() {
		operatorClient = &DefaultOperatorClient{}

		path := *common.WorkDir + "/conf/subscribe.conf"
		data, err := ioutil.ReadFile(path)
		if err != nil {
			log.Warnf("Failed to load config json from %s err %s", path, err.Error())
			return
		}

		if operatorConf.Type == "mpd" {
			operatorClient = &OperatorMpdClient{
				Type: operatorConf.Type,
			}
		} else if operatorConf.Type == "cloud" {
		} else {
			log.Fatalf("Unsupport subscribe type %s", operatorConf.Type)
		}

		err = operatorClient.LoadConf(data)
		if err != nil {
			log.Fatalf("Failed to load conf %s err %s", string(data), err.Error())
		}

		log.Infof("Success to load %s operator client", operatorConf.Type)
	})
	return operatorClient
}

func (m *ResourceManager) GetEsClient() *EsClient {
	return &EsClient{}
}

func (m *ResourceManager) GetSingleConn(ep common.EndPoint, timeout time.Duration) *pg.DB {
	db := pg.Connect(&pg.Options{
		Addr:            ep.String(),
		User:            GetClusterManagerConfig().Account.AuroraUser,
		Password:        GetClusterManagerConfig().Account.AuroraPassword,
		Database:        common.DefaultDatabase,
		PoolSize:        1,
		DialTimeout:     timeout,
		ReadTimeout:     timeout,
		WriteTimeout:    timeout,
		ApplicationName: "ClusterManager",
	})
	return db
}

func (m *ResourceManager) GetMetricsConn(ep common.EndPoint) *pg.DB {
	m.ConnMutex.Lock()
	defer m.ConnMutex.Unlock()
	if db, exist := m.MetricsConn[ep]; !exist {
		db = pg.Connect(&pg.Options{
			Addr:            ep.String(),
			User:            GetClusterManagerConfig().Account.AuroraUser,
			Password:        GetClusterManagerConfig().Account.AuroraPassword,
			Database:        common.DefaultDatabase,
			PoolSize:        1,
			DialTimeout:     time.Millisecond * time.Duration(*common.EngineMetricsTimeoutMs),
			ReadTimeout:     time.Millisecond * time.Duration(*common.EngineMetricsTimeoutMs),
			WriteTimeout:    time.Millisecond * time.Duration(*common.EngineMetricsTimeoutMs),
			ApplicationName: "ClusterManager",
		})
		m.MetricsConn[ep] = db
		return db
	} else {
		return db
	}
}

func (m *ResourceManager) GetEngineConn(ep common.EndPoint) *pg.DB {
	m.ConnMutex.Lock()
	defer m.ConnMutex.Unlock()
	if db, exist := m.Conn[ep]; !exist {
		db = pg.Connect(&pg.Options{
			Addr:            ep.String(),
			User:            GetClusterManagerConfig().Account.AuroraUser,
			Password:        GetClusterManagerConfig().Account.AuroraPassword,
			Database:        common.DefaultDatabase,
			PoolSize:        1,
			DialTimeout:     time.Millisecond * time.Duration(*common.EngineDetectTimeoutMs),
			ReadTimeout:     time.Millisecond * time.Duration(*common.EngineDetectTimeoutMs),
			WriteTimeout:    time.Millisecond * time.Duration(*common.EngineDetectTimeoutMs),
			ApplicationName: "ClusterManager",
		})
		m.Conn[ep] = db
		return db
	} else {
		return db
	}
}

func (m *ResourceManager) ResetEngineConn(ep common.EndPoint) {
	m.ConnMutex.Lock()
	defer m.ConnMutex.Unlock()
	if db, exist := m.Conn[ep]; exist {
		db.Close()
		delete(m.Conn, ep)
	}
}

func (m *ResourceManager) ResetMetricsConn(ep common.EndPoint) {
	m.ConnMutex.Lock()
	defer m.ConnMutex.Unlock()
	if db, exist := m.MetricsConn[ep]; exist {
		db.Close()
		delete(m.MetricsConn, ep)
	}
}

func (m *ResourceManager) GetSmartClientService() *smartclient_service.SmartClientService {
	return m.SmartClientService
}
