package resource

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/klog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type K8sClient struct {
	NamespaceName string
	ClusterName   string
}

func (c *K8sClient) ChangeServiceBackend(serviceName string, newRwPodName string) (string, error) {

	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return "", err
	}
	svc, err := client.CoreV1().Services(c.NamespaceName).Get(serviceName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	oldPodName := svc.Spec.Selector["apsara.metric.pod_name"]
	svc.Spec.Selector["apsara.metric.pod_name"] = newRwPodName
	_, err = client.CoreV1().Services(c.NamespaceName).Update(svc)
	if err != nil {
		return "", err
	}

	log.Infof("ChangeServiceBackend: change svc %v pod Selector from %v to %v", serviceName, oldPodName, newRwPodName)
	svcUpdateTimeStr := time.Now().String()
	return svcUpdateTimeStr, nil
}

func (c *K8sClient) WaitServiceChanged(serviceName string, newRwIP string, timeout int) (string, error) {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return "", err
	}
	for {

		svc, err := client.CoreV1().Services(c.NamespaceName).Get(serviceName, metav1.GetOptions{})
		if err != nil {
			return "", err
		}

		if IsPolarBoxMode() {
			type Backend struct {
				ClientIP string `json:"NodeClientIP"`
			}
			var backends []Backend

			//直连模式直接返回，不需要等backend
			enableVIPStr := svc.Annotations["service.beta.kubernetes.io/enable-vip"]
			if enableVIPStr != "" {
				vipOptionStr := strings.TrimSpace(strings.ToUpper(enableVIPStr))
				if vipOptionStr == "F" || vipOptionStr == "FALSE" || vipOptionStr == "N" || vipOptionStr == "NO" || vipOptionStr == "0" {
					return time.Now().String(), nil
				}
			}

			backendsStr := svc.Annotations["service.beta.kubernetes.io/pgsql-lb-backends"]
			err := json.Unmarshal([]byte(backendsStr), &backends)
			if err == nil {
				if len(backends) == 1 {
					if backends[0].ClientIP == newRwIP {
						svcBackendGetTimeStr := time.Now().String()
						return svcBackendGetTimeStr, nil
					}
				} else {
					log.Warnf("Failed to parse service.beta.kubernetes.io/pgsql-lb-backends %s not one backend", backendsStr)
				}
			} else if err != nil {
				log.Warnf("Failed to parse service.beta.kubernetes.io/pgsql-lb-backends %s err %s", backendsStr, err.Error())
			}
		} else {
			if svc.Annotations["service.beta.kubernetes.io/aliyun-loadbalancer-vip-nclist"] == newRwIP {
				return "", nil
			}
		}

		time.Sleep(time.Millisecond * 100)
		timeout--
		if timeout < 0 {
			break
		}
	}

	return "", errors.Errorf("Failed to wait service backend changed to %s, timeout %d", newRwIP, timeout)

}

var ZTime = time.Time{}

func (c *K8sClient) GetOperatorEndpoints() ([]common.EndPoint, time.Time, error) {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return nil, ZTime, err
	}

	var endpoints []common.EndPoint
	lastTime := ZTime

	if IsPolarPaasMode() {
		managerNamespace := GetClusterManagerConfig().ManagerServiceNamespace
		if managerNamespace == "" {
			managerNamespace = "kube-system"
		}
		serviceName := GetClusterManagerConfig().ManagerServiceName
		service, err := client.CoreV1().Services(managerNamespace).Get(serviceName, metav1.GetOptions{})
		if err != nil {
			return nil, ZTime, errors.Wrapf(err, "Failed to get service %s ns %s", serviceName, managerNamespace)
		}

		if len(service.Spec.Ports) != 0 {
			epStr := service.Spec.ClusterIP + ":" + strconv.Itoa(int(service.Spec.Ports[0].Port))
			ep, err := common.NewEndPoint(epStr)
			if err != nil {
				log.Warnf("Failed to get operator endpoint, invalid ep %s", epStr)
			}
			endpoints = append(endpoints, *ep)
		}

	} else {
		operatorLabel := GetClusterManagerConfig().OperatorLabel
		if operatorLabel == "" {
			operatorLabel = "db_instance_operator"
		}
		operatorNamespace := GetClusterManagerConfig().OperatorNamespace
		if operatorNamespace == "" {
			operatorNamespace = "kube-system"
		}
		pods, err := client.CoreV1().Pods(operatorNamespace).List(metav1.ListOptions{
			LabelSelector: "apsaradb_pod_type=" + operatorLabel,
		})
		if err != nil {
			return nil, ZTime, errors.Wrapf(err, "Failed to get operator endpoints ")
		}
		for _, pod := range pods.Items {
			if pod.GetCreationTimestamp().After(lastTime) {
				lastTime = pod.GetCreationTimestamp().Time
			}
			epStr := pod.Status.HostIP + ":" + pod.ObjectMeta.Labels["pod_port"]
			ep, err := common.NewEndPoint(epStr)
			if err != nil {
				log.Warnf("Failed to get operator endpoint, invalid ep %s", epStr)
				continue
			}
			endpoints = append(endpoints, *ep)
		}

	}

	if len(endpoints) != 0 {
		return endpoints, lastTime, nil
	} else {
		return nil, ZTime, errors.New("Failed to get operator endpoints, empty endpoints")
	}
}

func (c *K8sClient) GetEsEndpoints() ([]common.EndPoint, error) {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return nil, err
	}
	service, err := client.CoreV1().Services("polar-tianxiang").Get("elasticsearch-http", metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to get es endpoints ")
	}
	var endpoints []common.EndPoint
	epStr := service.Spec.ClusterIP + ":9200"
	ep, err := common.NewEndPoint(epStr)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to get es endpoint, invalid ep %s", epStr)
	}
	endpoints = append(endpoints, *ep)

	if len(endpoints) != 0 {
		return endpoints, nil
	} else {
		return nil, errors.New("Failed to get es endpoints, empty endpoints")
	}
}

func (c *K8sClient) GetRedLineEndpoints() (*common.EndPoint, error) {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return nil, err
	}
	service, err := client.CoreV1().Services("rds").Get("rds-redline-worker-log-api", metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to get rds-redline-worker-log-api endpoints ")
	}

	clusterIP := service.Spec.ClusterIP

	if len(service.Spec.Ports) < 1 {
		return nil, errors.Wrapf(err, "Failed to get rds-redline-worker-log-api endpoints : not port found")
	}
	clusterPort := service.Spec.Ports[0].Port
	return &common.EndPoint{
		Host: clusterIP,
		Port: fmt.Sprintf("%v", clusterPort),
	}, nil
}

func (c *K8sClient) GetRedLineBackupEndpoints() (*common.EndPoint, error) {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return nil, err
	}
	service, err := client.CoreV1().Services("rds").Get("rds-redline-worker-perf-api", metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to get rds-redline-worker-perf-api endpoints ")
	}
	clusterIP := service.Spec.ClusterIP

	if len(service.Spec.Ports) < 1 {
		return nil, errors.Wrapf(err, "Failed to get rds-redline-worker-perf-api endpoints : not port found")
	}
	clusterPort := service.Spec.Ports[0].Port
	return &common.EndPoint{
		Host: clusterIP,
		Port: fmt.Sprintf("%v", clusterPort),
	}, nil
}

func (c *K8sClient) SwitchStore(oldRwPodName string, newRwPodName string) error {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return err
	}
	oldRwPod, err := client.CoreV1().Pods(c.NamespaceName).Get(oldRwPodName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrapf(err, "Failed to get old rw pod %s", oldRwPodName)
	}

	for _, volume := range oldRwPod.Spec.Volumes {
		if volume.Name == "polarstore" {
			volume.PersistentVolumeClaim.ReadOnly = true
		}
	}

	_, err = client.CoreV1().Pods(c.NamespaceName).Update(oldRwPod)
	if err != nil {
		return errors.Wrapf(err, "Failed to update old rw pod %s", oldRwPodName)
	}

	newRwPod, err := client.CoreV1().Pods(c.NamespaceName).Get(newRwPodName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrapf(err, "Failed to get new rw pod %s", newRwPodName)
	}

	for _, volume := range newRwPod.Spec.Volumes {
		if volume.Name == "polarstore" {
			volume.PersistentVolumeClaim.ReadOnly = false
		}
	}

	_, err = client.CoreV1().Pods(c.NamespaceName).Update(newRwPod)
	if err != nil {
		return errors.Wrapf(err, "Failed to update new rw pod %s", newRwPodName)
	}

	log.Infof("Success to switch store on k8s from %s to %s", oldRwPodName, newRwPodName)

	return nil
}

func (c *K8sClient) GetPolarStore(pvcName string, store *PolarStore) error {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return err
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims(c.NamespaceName).Get(pvcName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	pv, err := client.CoreV1().PersistentVolumes().Get(pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	store.Uid = pv.Spec.CSI.VolumeAttributes["uid"]
	store.Endpoint = pv.Spec.CSI.VolumeAttributes["url"]
	store.PdbName = pvcName

	return nil
}

func (c *K8sClient) GetSlbIDFromK8s(serviceName string) (string, error) {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return "", err
	}
	_, err = client.CoreV1().Services(c.NamespaceName).Get(serviceName, metav1.GetOptions{})
	if err != nil {
		return "", errors.Wrapf(err, "Failed to get service %s:%s", c.NamespaceName, serviceName)
	}

	return "", nil
}

func (c *K8sClient) GetInsInfoFromK8s(podName string, ins *common.InsSpec) (*v1.Pod, error) {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return nil, err
	}

	pod, err := client.CoreV1().Pods(c.NamespaceName).Get(podName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if pod.Status.Phase != v1.PodRunning {
		return pod, errors.Errorf("Pod %s is %s not Running", podName, pod.Status.Phase)
	}
	ins.HostName = pod.Spec.NodeName
	ins.PodID = string(pod.UID)

	if IsPolarBoxOneMode() {
		vipEp, err := client.CoreV1().Endpoints(c.NamespaceName).Get(podName, metav1.GetOptions{})
		if err != nil {
			log.Warnf("Failed to get endpoint %s err %s", podName, err.Error())
			return pod, nil
		}
		for _, vip := range vipEp.Subsets {
			if len(vip.Addresses) == 0 || len(vip.Ports) == 0 {
				continue
			}
			vipStr := vip.Addresses[0].IP + ":" + strconv.Itoa(int(vip.Ports[0].Port))
			ep, err := common.NewEndPoint(vipStr)
			if err != nil {
				return pod, errors.Wrapf(err, "Invalid vip %s from endpoint %s", vipStr, podName)
			}
			ins.Endpoint = *ep
			break
		}
	} else if IsPolarBoxMulMode() {
		node, err := c.GetNodeInfo(pod.Spec.NodeName)
		if err != nil {
			return pod, errors.Wrapf(err, "Invalid node %s", pod.Spec.NodeName)
		}
		clientIP := ""
		for _, cond := range node.Status.Conditions {
			if cond.Type == "NodeClientIP" {
				clientIP = cond.Message
			}
		}
		ep, err := common.NewEndPoint(clientIP + ":" + ins.Port)
		if err != nil {
			return pod, errors.Wrapf(err, "Invalid endpoint %s from pod %s", clientIP+":"+ins.Port, podName)
		}
		ins.Endpoint = *ep
		ins.MaxMemSizeMB = pod.Spec.Containers[0].Resources.Limits.Memory().Value() / (1000 * 1000)

	} else if IsPolarCloudMode() {
		ep, err := common.NewEndPoint(pod.Status.HostIP + ":" + ins.Port)
		if err != nil {
			return pod, errors.Wrapf(err, "Invalid endpoint from pod %s", podName)
		}
		ins.Endpoint = *ep
		ins.MaxMemSizeMB = pod.Spec.Containers[0].Resources.Limits.Memory().Value() / (1000 * 1000)
	} else if IsPolarPaasMode() {
		ep, err := common.NewEndPoint(pod.Status.HostIP + ":" + ins.Port)
		if err != nil {
			return nil, errors.Wrapf(err, "Invalid endpoint from pod %s", podName)
		}
		ins.Endpoint = *ep
		ins.MaxMemSizeMB = pod.Spec.Containers[0].Resources.Limits.Memory().Value() / (1000 * 1000)
		ins.AuroraPassword = GetClusterManagerConfig().Account.AuroraPassword
	} else {
		log.Warnf("Failed to GetInsInfoFromK8s invalid work mode %s", GetClusterManagerConfig().Mode)
	}

	if ins.Endpoint.IsDefault() {
		log.Warnf("Empty endpoint for pod %s", podName)
	}

	return pod, nil
}

func (c *K8sClient) GetNodeInfo(node string) (*v1.Node, error) {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return nil, err
	}
	return client.CoreV1().Nodes().Get(node, metav1.GetOptions{})
}

func (c *K8sClient) GetPodInfo(podName string) (*v1.Pod, error) {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return nil, err
	}
	return client.CoreV1().Pods(c.NamespaceName).Get(podName, metav1.GetOptions{})
}

func (c *K8sClient) UpdatePodInfo(pod *v1.Pod) error {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return err
	}

	_, err = client.CoreV1().Pods(c.NamespaceName).Update(pod)
	return err
}

func (c *K8sClient) GetAlarmConfigMap() ([]common.AlarmInfo, error) {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return nil, err
	}
	var alarms []common.AlarmInfo
	config, err := client.CoreV1().ConfigMaps(c.NamespaceName).Get(c.ClusterName+common.AlarmConfigMapPostfix, metav1.GetOptions{})
	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			return nil, err
		}
	} else {
		if v, exist := config.Data["alarmSend"]; exist {
			err = json.Unmarshal([]byte(v), &alarms)
			if err != nil {
				return nil, err
			}
		}
	}

	return alarms, nil
}

func (c *K8sClient) UpdateAlarmConfigMap(alarmInfos []common.AlarmInfo) error {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return err
	}
	if len(alarmInfos) > 0 {
		data, err := json.Marshal(alarmInfos)
		if err != nil {
			return err
		}

		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: c.ClusterName + common.AlarmConfigMapPostfix,
				Labels: map[string]string{
					"InsAlarm": "true",
				},
			},
			Data: map[string]string{
				"alarmMsg": string(data),
			},
		}
		_, err = client.CoreV1().ConfigMaps(c.NamespaceName).Update(configMap)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				_, err = client.CoreV1().ConfigMaps(c.NamespaceName).Create(configMap)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	} else {
		err = client.CoreV1().ConfigMaps(c.NamespaceName).Delete(c.ClusterName+common.AlarmConfigMapPostfix, &metav1.DeleteOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *K8sClient) GetConfigMap() (*common.ClusterManagerConfig, error) {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return nil, err
	}
	var cmConfig common.ClusterManagerConfig
	config, err := client.CoreV1().ConfigMaps(c.NamespaceName).Get(c.ClusterName+common.CmConfigMapPostfix, metav1.GetOptions{})
	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			return nil, err
		}
	} else {
		if v, exist := config.Data["cmConfig"]; exist {
			err = json.Unmarshal([]byte(v), &cmConfig)
			if err != nil {
				return nil, err
			}
		}
	}

	return &cmConfig, nil
}

func (c *K8sClient) UpdateConfigMap(cmConfig *common.ClusterManagerConfig) error {
	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return err
	}
	data, err := json.Marshal(cmConfig)
	if err != nil {
		return err
	}

	configMap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: c.ClusterName + common.CmConfigMapPostfix,
		},
		Data: map[string]string{
			"cmConfig": string(data),
		},
	}
	_, err = client.CoreV1().ConfigMaps(c.NamespaceName).Update(configMap)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			_, err = client.CoreV1().ConfigMaps(c.NamespaceName).Create(configMap)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func (c *K8sClient) EnableClusterManager(enable bool) error {
	s, err := c.GetConfigMap()
	if err != nil {
		return err
	}

	if !enable {
		if s.Phase == common.DisableAllPhase {
			return nil
		}
		s.Phase = common.DisableAllPhase
	} else {
		if s.Phase == common.RunningPhase {
			return nil
		}
		if s.Phase != "" && s.Phase != common.DisableAllPhase {
			return errors.Errorf("ClusterManager is not in DisableAllPhase, phase=%s", s.Phase)
		}
		s.Phase = common.RunningPhase
	}

	return c.UpdateConfigMap(s)
}

func (c *K8sClient) AddSwitchEvent(event *common.SwitchEvent) error {

	client, _, err := GetDefaultKubeClient()
	if err != nil {
		return errors.Wrap(err, "Failed to get default client")
	}
	msg, err := json.Marshal(event)
	if err != nil {
		return errors.Wrapf(err, "Failed to marshal event %v", event)
	}

	_, err = client.CoreV1().ConfigMaps(c.NamespaceName).Create(&v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: GetClusterManagerConfig().Cluster.Name + "-switch-log-" + event.StartAt.Format("20060102150405"),
			Labels: map[string]string{
				"ClusterName": GetClusterManagerConfig().Cluster.Name,
				"Type":        "switch-log",
			},
		},
		Data: map[string]string{
			"log": string(msg),
		},
	})
	if err != nil {
		log.Warnf("Failed to add switch event %s err %s", msg, err.Error())
	} else {
		log.Infof("Success to add switch event %s", msg)
	}
	return nil
}

func GetDefaultKubeClient() (*kubernetes.Clientset, *rest.Config, error) {
	return GetKubeClient(0)
}

func GetKubeClient(timeout time.Duration) (*kubernetes.Clientset, *rest.Config, error) {
	// Use in-cluster config default
	for {
		// creates the in-cluster config
		config, err := rest.InClusterConfig()
		if err != nil {
			break
		}
		if timeout == 0 {
			if IsPolarBoxMode() {
				config.Timeout = time.Duration(6 * time.Second)
			} else {
				config.Timeout = time.Duration(180 * time.Second)
			}
		} else {
			config.Timeout = timeout
		}
		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			break
		}
		return clientset, config, nil
	}
	// support local test
	kubeconfig := filepath.Join(
		os.Getenv("HOME"), ".kube", "config",
	)
	if _, err := os.Stat(kubeconfig); err == nil {
		config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, nil, fmt.Errorf("could not get kubernetes client: %s", err)
		}
		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			return nil, nil, fmt.Errorf("could not get kubernetes client: %s", err)
		}

		return clientset, config, nil
	} else {
		return nil, nil, fmt.Errorf("could not get kubernetes client: %s", err)
	}
}

func ExecInPodWithRetry(command []string, containerName, podName, namespace string, times int, timeout time.Duration) (string, string, error) {
	retryTimes := 0
	for {
		stdout, stderr, err := ExecInPod(command, containerName, podName, namespace, timeout)
		if err != nil {
			log.Warnf("Exec %v InPod %s return %s, %s retry %d", command, podName, stdout, stderr, retryTimes)
			if retryTimes > times {
				return stdout, stderr, err
			}
			if IsPolarBoxMode() && strings.Contains(err.Error(), "not found") {
				return stdout, stderr, err
			}
			retryTimes++
			time.Sleep(time.Second * time.Duration(retryTimes))
		} else {
			return stdout, stderr, err
		}
	}
}

func ExecInPod(command []string, containerName, podName, namespace string, timeout time.Duration) (string, string, error) {
	_, config, err := GetDefaultKubeClient()
	if err != nil {
		return "", "", err
	}

	if timeout != 0 {
		config.Timeout = timeout
	}

	coreclient, err := corev1client.NewForConfig(config)
	if err != nil {
		log.Errorf("ExecInPod raise panic err: %v", err)
		panic(err)
	}
	req := coreclient.RESTClient().Post().
		Namespace(namespace).
		Resource("pods").
		Name(podName).
		SubResource("exec").
		VersionedParams(&v1.PodExecOptions{
			Container: containerName,
			Command:   command,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	klog.V(8).Info("Request URL:", req.URL().String())

	exec, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return "", "", fmt.Errorf("error while creating Executor: %v", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.Stream(remotecommand.StreamOptions{
		Stdin:  nil,
		Stdout: &stdout,
		Stderr: &stderr,
		Tty:    false,
	})
	stdOutStr := stdout.String()
	stdErrStr := stderr.String()
	if err != nil {
		log.Debugf("execute cmd failed: %v , std_out:%v std_err: %v", command, stdOutStr, stdErrStr)
		return stdOutStr, stdErrStr, err
	} else {
		log.Debugf("execute cmd success: %v , std_out:%v std_err: %v", command, stdOutStr, stdErrStr)
	}
	return stdOutStr, stdErrStr, nil
}
