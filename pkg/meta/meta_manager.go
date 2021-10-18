package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/notify"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/smartclient_service"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"runtime"
	"strings"
	"sync"
	"time"
)

type MetaManager struct {
	// 集群spec, 需要持久化
	ClusterSpec *common.ClusterSpecMeta

	// MaxScale spec, 从k8s订阅，或者由Meta触发更新不需要持久化
	ProxySpec *common.ProxySpec

	ProxyMeta *common.ProxyMeta

	// 集群的拓扑元数据，需要持久化
	ClusterMeta *common.ClusterStatusMeta

	RunningTask common.SwitchTask

	Vip map[string]common.VipSpec
	// 集群的配置及变更记录，需要持久化
	ClusterFileConf *common.ClusterFileConf

	// ClusterManager动态配置
	CmConf *common.ClusterManagerConf

	// 报警信息
	Alarm *common.Alarms

	clusterPhaseLock sync.Mutex
}

type VisualIns struct {
	EndPoint   string `json:"endpoint,omitempty"`
	PodName    string `json:"pod_name,omitempty"`
	DataPath   string `json:"data_path,omitempty"`
	User       string `json:"user,omitempty"`
	Phase      string `json:"phase,omitempty"`
	StartAt    string `json:"start_at,omitempty"`
	SyncStatus string `json:"sync_status,omitempty"`
	Role       string `json:"role,omitempty"`
}

type PluginStatus struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type UniverseExplorer struct {
}

type ProxyInfo struct {
}

type VisualCluster struct {
	Rw *VisualIns  `json:"rw,omitempty"`
	Ro []VisualIns `json:"ro,omitempty"`
}

type Visual struct {
	Phase          string           `json:"phase"`
	ClusterManager []VisualIns      `json:"cluster_manager,omitempty"`
	Master         *VisualIns       `json:"master,omitempty"`
	Standby        []VisualIns      `json:"standby,omitempty"`
	Rw             *VisualIns       `json:"rw,omitempty"`
	Ro             []VisualIns      `json:"ro,omitempty"`
	StandbyCluster []VisualCluster  `json:"standby_cluster,omitempty"`
	Ue             []VisualIns      `json:"universe_explorer,omitempty"`
	BackupAgent    []VisualIns      `json:"backup_agent,omitempty"`
	NodeDriver     []VisualIns      `json:"node_driver,omitempty"`
	Proxy          []VisualIns      `json:"proxy,omitempty"`
	Vip            []common.VipSpec `json:"vip,omitempty"`
	Plugins        []PluginStatus   `json:"plugins,omitempty"`
}

var onceMeta sync.Once
var mm *MetaManager

func GetMetaManager() *MetaManager {
	onceMeta.Do(func() {
		mm = &MetaManager{
			ClusterMeta: &common.ClusterStatusMeta{
				SubCluster: make(map[string]*common.SubClusterStatusMeta),
			},
			ProxySpec: &common.ProxySpec{
				InstanceSpec: make(map[common.EndPoint]*common.InsSpec),
			},
			ProxyMeta: &common.ProxyMeta{
				InstanceSpec: make(map[string]*common.InsSpec),
			},
			ClusterSpec: &common.ClusterSpecMeta{
				SubCluster: make(map[string]*common.SubClusterSpecMeta),
			},
			Alarm: &common.Alarms{
				Alarm: make(map[string]map[string]common.AlarmInfo),
			},
			Vip: make(map[string]common.VipSpec),
			ClusterFileConf: &common.ClusterFileConf{
				Conf: map[string]common.FileSetting{},
			},
			CmConf: &common.ClusterManagerConf{
				Settings: map[string]interface{}{},
			},
		}
	})
	return mm
}

func (m *MetaManager) Reset() error {
	m.ClusterMeta = &common.ClusterStatusMeta{
		SubCluster: make(map[string]*common.SubClusterStatusMeta),
	}

	m.ProxySpec = &common.ProxySpec{
		InstanceSpec: make(map[common.EndPoint]*common.InsSpec),
	}
	m.ProxyMeta = &common.ProxyMeta{
		InstanceSpec: make(map[string]*common.InsSpec),
	}
	m.ClusterSpec = &common.ClusterSpecMeta{
		SubCluster: make(map[string]*common.SubClusterSpecMeta),
	}
	m.Alarm = &common.Alarms{
		Alarm: make(map[string]map[string]common.AlarmInfo),
	}
	m.ClusterFileConf = &common.ClusterFileConf{
		Conf: map[string]common.FileSetting{},
	}
	m.CmConf = &common.ClusterManagerConf{
		Settings: map[string]interface{}{},
	}

	m.Vip = make(map[string]common.VipSpec)
	m.RunningTask = common.SwitchTask{}

	return nil
}

func (m *MetaManager) String() string {
	s := ""
	if spec, err := json.MarshalIndent(m.ClusterSpec, "", "\t"); err != nil {
		s += "Spec: " + err.Error() + "\n"
	} else {
		s += "Spec: " + string(spec) + "\n"
	}

	if meta, err := json.MarshalIndent(m.ClusterMeta, "", "\t"); err != nil {
		s += "Meta: " + err.Error()
	} else {
		s += "Meta: " + string(meta)
	}

	if proxy, err := json.MarshalIndent(m.ProxyMeta, "", "\t"); err != nil {
		s += "Proxy: " + err.Error()
	} else {
		s += "Proxy: " + string(proxy)
	}

	return s
}

func (m *MetaManager) GenerateVisualIns(cluster *common.SubClusterStatusMeta, clusterID, ep string) VisualIns {

	ins := VisualIns{
		EndPoint: ep,
		Phase:    cluster.EnginePhase[ep],
		StartAt:  cluster.EnginePhaseStartAt[ep].Local().Format("2006-01-02 15:04:05"),
	}

	if resource.IsPolarPureMode() || resource.IsPolarPaasMode() {
		c, exist := m.ClusterSpec.SubCluster[clusterID].InstanceSpec[ep]
		if exist {
			if c.IsRPMDeploy() {
				ins.DataPath = c.DataPath
				ins.User = c.EngineUser
			} else if c.IsDockerDeploy() {
				ins.PodName = c.PodName
			} else if c.IsNodeDriverDeploy() {

			}
			tag := m.GetEngineTag(c)
			if tag != nil {
				if v, exist := tag[common.SyncStatusTag]; exist {
					ins.SyncStatus = v
				}
			}

			if c.IsDataMax {
				ins.Role = string(common.DataMax)
			} else if c.SwitchPriority == -1 {
				ins.Role = string(common.Learner)
			}
		}
	}

	return ins
}

func (m *MetaManager) VisualString(v *Visual) {

	v.Phase = m.ClusterMeta.Phase

	for id, cluster := range m.ClusterMeta.SubCluster {
		if cluster.RwEndpoint.IsDefault() {
			continue
		}
		if cluster.ClusterType == common.PaxosCluster || cluster.ClusterType == common.SharedNothingCluster {
			ins := m.GenerateVisualIns(cluster, id, cluster.RwEndpoint.String())
			if id != m.ClusterMeta.MasterClusterID {
				v.Standby = append(v.Standby, ins)
			} else {
				v.Master = &ins
			}
		} else if cluster.ClusterType == common.SharedStorageCluster {
			if id == m.GetMasterClusterID() {
				for ep, _ := range cluster.EnginePhase {
					ins := m.GenerateVisualIns(cluster, id, ep)
					if ins.EndPoint == cluster.RwEndpoint.String() {
						v.Rw = &ins
					} else {
						v.Ro = append(v.Ro, ins)
					}
				}
			} else {
				var standbyCluster VisualCluster
				for ep, _ := range cluster.EnginePhase {
					ins := m.GenerateVisualIns(cluster, id, ep)
					if ins.EndPoint == cluster.RwEndpoint.String() {
						standbyCluster.Rw = &ins
					} else {
						standbyCluster.Ro = append(standbyCluster.Ro, ins)
					}
				}

				v.StandbyCluster = append(v.StandbyCluster, standbyCluster)
			}
		}
	}

	for _, vip := range m.Vip {
		v.Vip = append(v.Vip, vip)
	}

}

func (m *MetaManager) GetRunningTask() common.SwitchTask {
	return m.RunningTask
}

func (m *MetaManager) SyncAlarms() error {
	alarms := m.GetAlarms()
	v, err := json.Marshal(alarms)
	if err != nil {
		return errors.Wrapf(err, "Failed to marshal alarms %v", alarms)
	}

	return GetConsensusService().Set(ClusterManagerAlarmKey, v)
}

func (m *MetaManager) GetAlarms() []common.AlarmInfo {
	var alarms []common.AlarmInfo
	for _, insAlarm := range m.Alarm.Alarm {
		for _, alarm := range insAlarm {
			alarms = append(alarms, alarm)
		}
	}
	return alarms
}

func (m *MetaManager) AddAlarm(alarm common.AlarmInfo) bool {
	changed := false
	if insAlarm, exist := m.Alarm.Alarm[alarm.Spec.Endpoint.String()]; exist {
		if _, exist := insAlarm[alarm.Type]; !exist {
			insAlarm[alarm.Type] = alarm
			changed = true
		}
	} else {
		m.Alarm.Alarm[alarm.Spec.Endpoint.String()] = map[string]common.AlarmInfo{
			alarm.Type: alarm,
		}
		changed = true
	}

	return changed
}

func (m *MetaManager) IsAlarmExist(alarmType string, spec *common.InsSpec) bool {
	if insAlarm, exist := m.Alarm.Alarm[spec.Endpoint.String()]; exist {
		if _, exist := insAlarm[alarmType]; exist {
			return true
		}
	}
	return false
}

func (m *MetaManager) RemoveAlarm(alarm common.AlarmInfo) bool {
	changed := false
	if insAlarm, exist := m.Alarm.Alarm[alarm.Spec.Endpoint.String()]; exist {
		if _, exist := insAlarm[alarm.Type]; exist {
			delete(insAlarm, alarm.Type)
			changed = true
		}
		if len(insAlarm) == 0 {
			delete(m.Alarm.Alarm, alarm.Spec.Endpoint.String())
			changed = true
		}
	}
	return changed
}

func (m *MetaManager) RemoveInsAlarm(spec *common.InsSpec) bool {
	changed := false
	if _, exist := m.Alarm.Alarm[spec.Endpoint.String()]; exist {
		delete(m.Alarm.Alarm, spec.Endpoint.String())
		changed = true
	}
	return changed
}

func (m *MetaManager) WaitInsRemove(spec *common.InsSpec, timeout time.Duration, lock sync.Locker) error {
	ch := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	go func() {
		for {
			select {
			case <-ctx.Done():
				ch <- errors.New("Wait Ins Remove timeout")
				return
			case <-time.After(time.Millisecond * 500):
				lock.Lock()
				if subCluster, exist := m.ClusterMeta.SubCluster[spec.ClusterID]; !exist {
					lock.Unlock()
					ch <- nil
					return
				} else {
					if _, exist := subCluster.EnginePhase[spec.Endpoint.String()]; !exist {
						lock.Unlock()
						ch <- nil
						return
					}
				}
				lock.Unlock()
			}
		}
	}()

	return <-ch
}

func (m *MetaManager) WaitInsReady(spec *common.InsSpec, timeout time.Duration, lock sync.Locker) error {
	ch := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	go func() {
		for {
			select {
			case <-ctx.Done():
				msg := ""
				lock.Lock()
				if insAlarm, exist := m.Alarm.Alarm[spec.Endpoint.String()]; exist {
					if alarm, exist := insAlarm[common.AlarmActionExecFailed]; exist {
						msg = alarm.Message
					}
				}
				lock.Unlock()
				if msg != "" {
					ch <- errors.Errorf("Wait Ins Ready timeout err %s", msg)
				} else {
					ch <- errors.New("Wait Ins Ready timeout")
				}
				return
			case <-time.After(time.Millisecond * 500):
				lock.Lock()
				if subCluster, exist := m.ClusterMeta.SubCluster[spec.ClusterID]; exist {
					if phase, exist := subCluster.EnginePhase[spec.Endpoint.String()]; exist {
						if phase == common.EnginePhaseRunning {
							ch <- nil
							lock.Unlock()
							return
						}
					}
				}
				lock.Unlock()
			}

		}
	}()

	return <-ch
}

func (m *MetaManager) GetInsPhase(spec *common.InsSpec) (string, time.Time, error) {
	if subCluster, exist := m.ClusterMeta.SubCluster[spec.ClusterID]; exist {
		if phase, exist := subCluster.EnginePhase[spec.Endpoint.String()]; exist {
			return phase, subCluster.EnginePhaseStartAt[spec.Endpoint.String()], nil
		}
	}
	return "", time.Now(), errors.Errorf("Failed to get ins %s phase, not exist", spec.String())
}

func (m *MetaManager) CreateInsPhase(spec *common.InsSpec, insPhase string) {
	m.ClusterMeta.SubCluster[spec.ClusterID].EnginePhase[spec.Endpoint.String()] = insPhase
	m.ClusterMeta.SubCluster[spec.ClusterID].EnginePhaseStartAt[spec.Endpoint.String()] = time.Now()
}

func (m *MetaManager) RemoveInsPhase(spec *common.InsSpec) {
	delete(m.ClusterMeta.SubCluster[spec.ClusterID].EnginePhase, spec.Endpoint.String())
	delete(m.ClusterMeta.SubCluster[spec.ClusterID].EnginePhaseStartAt, spec.Endpoint.String())
}

func (m *MetaManager) SetInsPhase(spec *common.InsSpec, insPhase string) error {
	_, fileName, line, ok := runtime.Caller(1)
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
	head := fmt.Sprintf("%v:%v", short, line)

	if subCluster, exist := m.ClusterMeta.SubCluster[spec.ClusterID]; exist {
		if _, exist := subCluster.EnginePhase[spec.Endpoint.String()]; exist {
			log.Infof("[%s: notify]Success to switch engine %s phase from %s to %s",
				head, spec.String(), subCluster.EnginePhase[spec.Endpoint.String()], insPhase)

			rEvent := notify.BuildDBEventPrefixByInsV2(spec, common.RwEngine, spec.Endpoint)
			rEvent.Body.Describe = fmt.Sprintf("Instance %v status change , po : %v : %v->%v", spec.CustID, spec.PodName, subCluster.EnginePhase[spec.Endpoint.String()], insPhase)
			if insPhase != "RUNNING" {
				rEvent.EventCode = notify.EventCode_InsNotRunning
				rEvent.Level = notify.EventLevel_ERROR
			} else {
				rEvent.EventCode = notify.EventCode_InsRunning
				rEvent.Level = notify.EventLevel_INFO
			}

			sErr := notify.MsgNotify.SendMsgToV2(&rEvent)
			if sErr != nil {
				log.Errorf("StartSwitchTaskV2 SendMsgToV2 err: %v", sErr)
			}
			subCluster.EnginePhase[spec.Endpoint.String()] = insPhase
			subCluster.EnginePhaseStartAt[spec.Endpoint.String()] = time.Now()
			return m.Sync()
		}
	}
	return errors.Errorf("Failed to set ins %s phase, not exist", spec.String())
}

func (m *MetaManager) AddEngineTag(spec *common.InsSpec, tag map[string]string) {
	t := m.GetEngineTag(spec)
	if t != nil {
		for k, v := range tag {
			t[k] = v
		}
		m.Sync()
	}
}

func (m *MetaManager) RemoveEngineTag(spec *common.InsSpec, tag map[string]string) {
	t := m.GetEngineTag(spec)
	if t != nil {
		for k, _ := range tag {
			delete(t, k)
		}
		m.Sync()
	}
}

func (m *MetaManager) GetEngineTag(spec *common.InsSpec) map[string]string {
	if c, exist := m.ClusterMeta.SubCluster[spec.ClusterID]; exist {
		if t, exist := c.EngineTag[spec.Endpoint.String()]; exist {
			return t.Tags
		} else {
			c.EngineTag[spec.Endpoint.String()] = &common.TagMap{
				Tags: make(map[string]string),
			}
			return c.EngineTag[spec.Endpoint.String()].Tags
		}
	} else {
		return nil
	}
}

func (m *MetaManager) GetInsSpec(endpoint *common.EndPoint) (*common.InsSpec, error) {
	for _, subCluster := range m.ClusterSpec.SubCluster {
		if insSpec, exist := subCluster.InstanceSpec[endpoint.String()]; exist && !endpoint.IsDefault() {
			return insSpec, nil
		}
	}
	log.Warnf("Failed to get ins spec %s not exist", endpoint.String())
	return nil, errors.Errorf("Not exist spec %s", endpoint.String())
}

func (m *MetaManager) GetEnginePhase(endpoint common.EndPoint) (string, error) {
	for _, subcluster := range m.ClusterMeta.SubCluster {
		if phase, exist := subcluster.EnginePhase[endpoint.String()]; exist {
			return phase, nil
		}
	}

	log.Warnf("Failed to get ins %s phase not exist", endpoint.String())
	return "", errors.Errorf("ins %s phase not exist", endpoint.String())
}

func (m *MetaManager) GetRwEnginePhase() (string, time.Time) {
	return m.ClusterMeta.SubCluster[m.ClusterMeta.MasterClusterID].EnginePhase[m.ClusterMeta.SubCluster[m.ClusterMeta.MasterClusterID].RwEndpoint.String()],
		m.ClusterMeta.SubCluster[m.ClusterMeta.MasterClusterID].EnginePhaseStartAt[m.ClusterMeta.SubCluster[m.ClusterMeta.MasterClusterID].RwEndpoint.String()]
}

func (m *MetaManager) GetClusterReadOnly() bool {
	if rwTag := m.GetClusterTag(); rwTag != nil {
		if _, exist := rwTag.Tags["readonly"]; exist {
			return true
		}
	}
	return false
}

func (m *MetaManager) SetClusterReadOnlyTag() {
	if rwTag := m.GetClusterTag(); rwTag != nil {
		rwTag.Tags["readonly"] = "true"
	}
}

func (m *MetaManager) RemoveClusterReadOnlyTag() {
	if rwTag := m.GetClusterTag(); rwTag != nil {
		delete(rwTag.Tags, "readonly")
	}
}

func (m *MetaManager) GetClusterTag() *common.TagMap {
	if masterCluster, exist := m.ClusterMeta.SubCluster[m.ClusterMeta.MasterClusterID]; exist {
		if masterCluster.ClusterTag.Tags == nil {
			masterCluster.ClusterTag.Tags = make(map[string]string)
		}
		return &masterCluster.ClusterTag
	}
	return nil
}

func (m *MetaManager) GetPhase() string {
	m.clusterPhaseLock.Lock()
	defer m.clusterPhaseLock.Unlock()

	return m.ClusterMeta.Phase
}

/*
 * RunningPhase <-> DisableAllPhase, SwitchingPhase
 *
 */
func (m *MetaManager) SwitchPhase(toPhase string) error {
	m.clusterPhaseLock.Lock()
	defer m.clusterPhaseLock.Unlock()

	fromPhase := m.ClusterMeta.Phase
	if m.ClusterMeta.Phase == toPhase {
		return nil
	}

	m.ClusterMeta.Phase = toPhase

	log.Infof("ClusterManager Phase switch from %s to %s", fromPhase, toPhase)
	return m.Sync()
}

func (m *MetaManager) SetLockReadOnly(v bool) error {
	m.ClusterSpec.ReadOnly = v
	return m.Sync()
}

func (m *MetaManager) SetStopped(v bool) error {
	m.ClusterSpec.Stopped = v
	return m.Sync()
}

func (m *MetaManager) GetStopped() bool {
	return m.ClusterSpec.Stopped
}

func (m *MetaManager) GetSpecLockReadOnly() bool {
	return m.ClusterSpec.ReadOnly
}

func (m *MetaManager) GetSpecMasterClusterID() string {
	return m.ClusterSpec.InitMasterClusterID
}

func (m *MetaManager) SetSpecMasterClusterID(clusterID string) {
	m.ClusterSpec.InitMasterClusterID = clusterID
}

func (m *MetaManager) SetMasterClusterID(clusterID string) {
	m.ClusterMeta.MasterClusterID = clusterID
}

func (m *MetaManager) GetMasterClusterID() string {
	if m.ClusterMeta.MasterClusterID == "" {
		return m.GetSpecMasterClusterID()
	} else {
		return m.ClusterMeta.MasterClusterID
	}
}

func (m *MetaManager) SetRwSpec(spec *common.InsSpec) {
	m.ClusterMeta.SubCluster[spec.ClusterID].RwEndpoint = spec.Endpoint
}

func (m *MetaManager) GetRwSpec(clusterID string) (*common.InsSpec, error) {
	if cluster, exist := m.ClusterMeta.SubCluster[clusterID]; exist {
		return m.GetInsSpec(&cluster.RwEndpoint)
	} else {
		return nil, errors.Errorf("Not exist cluster %s", clusterID)
	}
}

func (m *MetaManager) AddProxySpec(spec *common.InsSpec) error {
	if _, exist := m.ProxyMeta.InstanceSpec[spec.Endpoint.String()]; !exist {
		log.Infof("MetaManager add proxy %s", spec.String())
		m.ProxyMeta.InstanceSpec[spec.Endpoint.String()] = spec
	} else {
		return errors.Errorf("proxy %s already exist!", spec.String())
	}

	return m.Sync()
}

func (m *MetaManager) RemoveProxySpec(spec *common.InsSpec) error {
	if _, exist := m.ProxyMeta.InstanceSpec[spec.Endpoint.String()]; exist {
		log.Infof("MetaManager remove proxy %s", spec.String())
		delete(m.ProxyMeta.InstanceSpec, spec.Endpoint.String())
	}

	return m.Sync()
}

func (m *MetaManager) AddInsSpec(spec *common.InsSpec, engineType common.EngineType) error {
	subCluster, exist := m.ClusterSpec.SubCluster[spec.ClusterID]
	if !exist {
		m.ClusterSpec.SubCluster[spec.ClusterID] = &common.SubClusterSpecMeta{
			ClusterID:    spec.ClusterID,
			InstanceSpec: make(map[string]*common.InsSpec),
		}
		subCluster = m.ClusterSpec.SubCluster[spec.ClusterID]
	}

	if engineType == common.RwEngine {
		if m.ClusterSpec.InitMasterClusterID != "" {
			return errors.Errorf("Add rw ins only once, already exist cluster:%s", m.ClusterSpec.InitMasterClusterID)
		}
		if s, exist := subCluster.InstanceSpec[spec.Endpoint.String()]; !exist {
			m.ClusterSpec.InitMasterClusterID = spec.ClusterID
			subCluster.InitRwID = spec.ID
			subCluster.InstanceSpec[spec.Endpoint.String()] = spec
			subCluster.ClusterType = spec.ClusterType
		} else {
			if s.ClusterType != spec.ClusterType {
				return errors.Errorf("Failed to add ins %s as %s cluster type %s exist cluster %s",
					spec.String(), engineType, spec.ClusterType, s.ClusterType)
			} else {
				log.Infof("add ins %s as %s already exist skip it", spec.String(), engineType)
			}
		}
	} else if engineType == common.RoEngine {
		if m.ClusterSpec.InitMasterClusterID != "" {
			if orgSpec, exist := subCluster.InstanceSpec[spec.Endpoint.String()]; !exist {
				if spec.ClusterType == subCluster.ClusterType {
					subCluster.InstanceSpec[spec.Endpoint.String()] = spec
				} else {
					return errors.Errorf("Failed to add ins %s as %s cause cluster type not match new %s exist %s",
						spec.String(), engineType, spec.ClusterType, subCluster.ClusterType)
				}
			} else {
				orgPodName := orgSpec.PodName
				if orgPodName != spec.PodName {
					// pod名称发生变化？
					log.Infof(" found ro[%v] engine pod changed(%v->%v), but endpoint not changed. set prev info", spec.Endpoint.String(), orgPodName, spec.PodName)
					orgSpec.PrevSpec = &common.InsPrevSpec{
						PodName:  orgPodName,
						PodID:    orgSpec.PodID,
						Endpoint: orgSpec.Endpoint,
					}
					orgSpec.PodName = spec.PodName
					orgSpec.PodID = spec.PodID
					if orgSpec.IsDeleted {
						orgSpec.IsDeleted = false
					}
				} else {
					log.Infof("add ins %s as %s already exist skip it", spec.String(), engineType)
				}
			}
		} else {
			return errors.Errorf("Failed to add ins %s as %s cause rw not exist", spec.String(), engineType)
		}
	} else if engineType == common.StandbyEngine {
		if m.ClusterSpec.InitMasterClusterID != "" {
			if _, exist := subCluster.InstanceSpec[spec.Endpoint.String()]; !exist {
				subCluster.InitRwID = spec.ID
				subCluster.InstanceSpec[spec.Endpoint.String()] = spec
				subCluster.ClusterType = spec.ClusterType
			} else {
				log.Infof("add ins %s as %s already exist skip it", spec.String(), engineType)
			}
		} else {
			return errors.Errorf("Failed to add %s rw not exist", engineType)
		}
	} else {
		return errors.Errorf("add invalid ins type %s", engineType)
	}

	return m.Sync()
}

func (m *MetaManager) RemoveInsSpec(spec *common.InsSpec, isDeleted bool) error {
	if m.ClusterMeta.MasterClusterID == spec.ClusterID && isDeleted {
		if m.ClusterMeta.SubCluster[spec.ClusterID].RwEndpoint == spec.Endpoint {
			return errors.Errorf("Failed to remove ins %s, is rw", spec.String())
		}
	}

	subCluster, exist := m.ClusterSpec.SubCluster[spec.ClusterID]
	if !exist {
		return errors.Errorf("Failed to remove cluster %s, not exist ", spec.String())
	}
	insSpec, exist := subCluster.InstanceSpec[spec.Endpoint.String()]
	if !exist {
		return errors.Errorf("Failed to remove ins %s, not exist", spec.String())
	}

	insSpec.IsDeleted = isDeleted
	if spec.ID == subCluster.InitRwID && spec.ClusterID != m.ClusterMeta.MasterClusterID {
		insSpec.IsStandby = true
	}

	return m.Sync()
}

func (m *MetaManager) Reload() error {
	var c common.ClusterManagerConfig

	v, err := GetConsensusService().Get(ClusterManagerConfigKey)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil
		} else {
			return err
		}
	}
	log.Infof("Get config %s", v)
	err = json.Unmarshal(v, &c)
	if err != nil {
		return errors.Wrapf(err, "%v is not ClusterManagerConfig type", v)
	}

	if c.Spec != "" {
		if err := json.Unmarshal([]byte(c.Spec), m.ClusterSpec); err != nil {
			return errors.Wrapf(err, "Failed to marshal cluster spec %s", c.Spec)
		}
	}

	if c.Meta != "" {
		if err := json.Unmarshal([]byte(c.Meta), m.ClusterMeta); err != nil {
			return errors.Wrapf(err, "Failed to marshal cluster meta %s", c.Meta)
		}
	}

	if c.ProxyMeta != "" {
		if err := json.Unmarshal([]byte(c.ProxyMeta), m.ProxyMeta); err != nil {
			return errors.Wrapf(err, "Failed to marshal proxy meta %s", c.ProxyMeta)
		}
	}

	if c.CmConf != "" {
		if err := json.Unmarshal([]byte(c.CmConf), m.CmConf); err != nil {
			return errors.Wrapf(err, "Failed to unmarshal cm conf %s", c.CmConf)
		}
	}

	m.RunningTask = c.RunningTask
	for _, vip := range c.Vip {
		m.Vip[vip.Vip] = vip
	}

	log.Infof("reload meta manager %s", m.String())

	return nil
}

func (m *MetaManager) Sync() error {
	var s common.ClusterManagerConfig

	s.Phase = m.ClusterMeta.Phase
	s.RunningTask = m.RunningTask

	for _, vip := range m.Vip {
		s.Vip = append(s.Vip, vip)
	}

	if metaJson, err := json.Marshal(m.ClusterMeta); err != nil {
		return errors.Wrapf(err, "Failed to marshal cluster meta %s", m.String())
	} else {
		s.Meta = string(metaJson)
	}

	if specJson, err := json.Marshal(m.ClusterSpec); err != nil {
		return errors.Wrapf(err, "Failed to marshal cluster spec %s", m.String())
	} else {
		s.Spec = string(specJson)
	}

	if proxyJson, err := json.Marshal(m.ProxyMeta); err != nil {
		return errors.Wrapf(err, "Failed to marshal proxy meta %v", m.ProxyMeta)
	} else {
		s.ProxyMeta = string(proxyJson)
	}

	if cmConfJson, err := json.Marshal(m.CmConf); err != nil {
		return errors.Wrapf(err, "Failed to marshal cm conf %v", m.CmConf)
	} else {
		s.CmConf = string(cmConfJson)
	}

	v, err := json.Marshal(s)
	if err != nil {
		return errors.Wrapf(err, "Failed to marshal cluster manager config %v", s)
	}

	log.Infof("Sync meta %s", v)

	return GetConsensusService().Set(ClusterManagerConfigKey, v)
}

func (m *MetaManager) CreateSwitchTask(ac common.SwitchAction) error {
	now := v1.Now()
	ac.DeepCopyInto(&m.RunningTask.Action)
	m.RunningTask.StartedAt = &now
	m.RunningTask.FinishAt = &now
	m.RunningTask.Phase = common.SwitchingPhase

	return m.Sync()
}

func (m *MetaManager) FinishSwitchTask() error {
	var cm common.SwitchTask
	m.RunningTask = cm

	return m.Sync()
}

func (m *MetaManager) AddSwitchEvent(event *common.SwitchEvent) error {
	msg, err := json.Marshal(event)
	if err != nil {
		return errors.Wrapf(err, "Failed to marshal event %v", event)
	}

	err = GetConsensusService().Set(ClusterManagerSwitchLogKey, msg)
	if err != nil {
		log.Warnf("Failed to set switch event %s err %s", msg, err.Error())
	} else {
		log.Infof("Success to set switch event %s", msg)
	}
	return err
}

func (m *MetaManager) AddVip(vip, _interface, mask string, spec *common.InsSpec) error {
	if _, exist := m.Vip[vip]; !exist {
		m.Vip[vip] = common.VipSpec{
			Vip:       vip,
			Interface: _interface,
			Mask:      mask,
			Endpoint:  spec.Endpoint.String(),
		}
		return m.Sync()
	} else {
		return nil
	}
}

func (m *MetaManager) RemoveVip(vip string) error {
	delete(m.Vip, vip)

	return m.Sync()
}

func (m *MetaManager) GetVips() []common.VipSpec {
	var vips []common.VipSpec
	for _, vip := range m.Vip {
		vips = append(vips, vip)
	}
	return vips
}

// GetTopologyFromMetaManager 从MetaManager转换成拓扑
// 比对之前拓扑,有则更新,推送消息;无则不推送
func (m *MetaManager) GetTopologyFromMetaManager() *smartclient_service.TopologyEvent {
	topology := &smartclient_service.TopologyEvent{}
	for clusterID, cluster := range m.ClusterMeta.SubCluster {
		topologyNode := &smartclient_service.TopologyNode{}
		if clusterID == m.ClusterMeta.MasterClusterID {
			topologyNode.IsMaster = true
		}

		// RW节点直接记录在cluster的RwEndpoint
		topologyNode.Rw = []*smartclient_service.DBNode{
			&smartclient_service.DBNode{
				Endpoint: cluster.RwEndpoint.String(),
			},
		}

		// RO节点要遍历EnginePhase，key为ip:port，value为运行状态
		for host, phrase := range cluster.EnginePhase {
			if host == cluster.RwEndpoint.String() {
				// 从所有节点中剔除RW节点
				continue
			}
			if phrase == common.EnginePhaseRunning {
				// 需要判断运行状态是否为RUNNING
				topologyNode.Ro = append(topologyNode.Ro, &smartclient_service.DBNode{
					Endpoint: host,
				})
			}
		}

		for endpoint := range m.ProxySpec.InstanceSpec {
			// 补全Proxy
			ipport := endpoint.String()
			topologyNode.Maxscale = append(topologyNode.Maxscale, &smartclient_service.MaxScale{
				Endpoint: ipport,
			})
		}

		topology.Topology = append(topology.Topology, topologyNode)
	}

	return topology
}

func (m *MetaManager) GetCmConf(key string) (interface{}, error) {
	if v, exist := m.CmConf.Settings[key]; exist {
		return v, nil
	} else {
		return nil, errors.Errorf("cm conf %s not exist", key)
	}
}

func (m *MetaManager) AddCmConf(key string, value interface{}) error {
	m.CmConf.Settings[key] = value

	return m.Sync()
}

func (m *MetaManager) RemoveCmConf(key string) error {
	delete(m.CmConf.Settings, key)

	return m.Sync()
}
