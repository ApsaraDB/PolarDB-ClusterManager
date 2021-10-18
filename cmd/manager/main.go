package main

import (
	"context"
	"flag"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/manager"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/plugin"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/service"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/version"
	"go.etcd.io/etcd/pkg/flags"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

var apiService *service.ApiService

func init() {

	notify.GetClusterNameFn = func() string {
		return resource.GetClusterManagerConfig().Cluster.Name
	}

	notify.GetRedLineEpStr = func() (string, error) {
		ep, err := resource.GetResourceManager().GetK8sClient().GetRedLineEndpoints()
		if err != nil {
			return "", err
		}
		return ep.String(), nil
	}

	notify.GetRedLineBackupEpStr = func() (string, error) {
		ep, err := resource.GetResourceManager().GetK8sClient().GetRedLineBackupEndpoints()
		if err != nil {
			return "", err
		}
		return ep.String(), nil
	}
}

func main() {
	flag.Parse()

	flags.SetFlagsFromEnv("POLAR", flag.CommandLine)

	if *common.EnableDebugLog {
		log.SetLevel(log.LOG_LEVEL_DEBUG)
	} else {
		log.SetLevel(log.LOG_LEVEL_INFO)
	}

	err := resource.GetResourceManager().Initialize()
	if err != nil {
		log.Fatalf("Failed to initialize resource manager. err=%v", err)
		return
	}
	defer resource.GetResourceManager().Finalize()

	log.Infof("----------------------------------------------------------------------------------------------------------")
	log.Infof("-------------------          cm started                                                -------------------")
	log.Infof("----------------------------------------------------------------------------------------------------------")
	log.Infof("|                                                                                           |")
	log.Infof("| branch:%v commitId:%v ", version.GitBranch, version.GitCommitId)
	log.Infof("| repo %v", version.GitCommitRepo)
	log.Infof("| commitDate/buildDate %v , build Date: %v, User: %v, host=%v", version.GitCommitDate, version.BuildDate, version.BuildUser, version.BuildHost)
	log.Infof("|                                                                                           |")
	log.Infof("----------------------------------------------------------------------------------------------------------")

	log.Infof("DebugLog enabled=%v", *common.EnableDebugLog)

	apiService = service.NewApiService(resource.GetClusterManagerConfig().Cluster.Port)
	apiService.Start()
	defer apiService.Stop()

	notifyManger := notify.MsgNotify
	go func(n *notify.MsgNotifyManager) {
		n.Start(resource.GetLocalServerAddr(false), resource.IsPolarBoxMode, func() int {
			return resource.GetClusterManagerConfig().Cluster.Port
		})
	}(notifyManger)

	defer notifyManger.Close()

	RunConsensusService()
}

func RunConsensusService() {
	pluginManager := plugin.GetPluginManager(&apiService.ServiceCtrl)

	enableRecovery := resource.GetClusterManagerConfig().Consensus.Recovery

	consensusService := meta.GetConsensusService()
	ctx, cancel := context.WithCancel(context.Background())

	identify := resource.GetLocalServerAddr(false) + ":" + strconv.Itoa(resource.GetClusterManagerConfig().Cluster.Port)
	ListenSignals(cancel)

	var lock resourcelock.Interface
	if !resource.GetClusterManagerConfig().Consensus.EtcdStorage {
		lock = &resourcelock.ConfigMapLock{
			LockConfig: resourcelock.ResourceLockConfig{
				Identity: identify,
			},
		}
	} else {
		k8sClient, _, err := resource.GetDefaultKubeClient()
		if err != nil {
			log.Fatalf("Failed to get default client err=%s", err.Error())
		}

		lock = &resourcelock.ConfigMapLock{
			ConfigMapMeta: v1.ObjectMeta{
				Name:      resource.GetClusterManagerConfig().Cluster.Name + common.CmLeaderElectionPostfix,
				Namespace: resource.GetClusterManagerConfig().Cluster.Namespace,
			},
			Client: k8sClient.CoreV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				Identity: identify,
			},
		}
	}

	err := consensusService.Run(enableRecovery, ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   32 * time.Second,
		RenewDeadline:   30 * time.Second,
		RetryPeriod:     5 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// we're notified when we start - this is where you would
				// usually put your code
				log.Infof("%s get leader!", identify)
				pluginManager.Load(*common.PluginConfPath)

				apiService.ServiceCtrl.IsLeader = true
				StartLeaderLoop()
			},
			OnStoppedLeading: func() {
				// we can do cleanup here
				log.Infof("%s lost leader, exit...", identify)
				manager.GetClusterManager().Reset()

				apiService.ServiceCtrl.IsLeader = false
				pluginManager.Stop()
			},
			OnNewLeader: func(id string) {
				// we're notified when new leader elected
				if apiService.ServiceCtrl.Leader != id {
					apiService.ServiceCtrl.Leader = id
					log.Infof("new leader elected: %s", id)
				}
			},
		},
	})

	if err != nil {
		log.Fatalf("Failed to run consensus service err %s", err.Error())
	}
}

func StartLeaderLoop() {

	go func() {
		clusterManager := manager.GetClusterManager()
		clusterManager.Initialize()
		clusterManager.Start()

		for {
			select {
			case <-time.After(time.Second):
				if common.StopFlag {
					clusterManager.Stop()
					return
				}
			}
		}
	}()
}

func ListenSignals(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGSTOP, syscall.SIGKILL, syscall.SIGTERM)

	go func() {
		for {
			sig := <-ch
			if syscall.SIGSTOP == sig || syscall.SIGINT == sig || syscall.SIGKILL == sig || syscall.SIGTERM == sig {
				log.Info("Receive signal ", sig)
				common.StopFlag = true
				log.Info("PolarDB ClusterManager start stopping!")
				cancel()
				return
			}
		}
	}()
}
