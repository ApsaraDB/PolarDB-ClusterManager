package manager

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/decision"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/plugin"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/version"
	"strconv"
	"time"
)

func (m *ClusterManager) VisualStatus() string {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()
	v := &meta.Visual{}
	m.MetaManager.VisualString(v)
	m.StatusManager.VisualString(v)
	plugin.GetPluginManager(nil).Status(v)

	member := meta.GetConsensusService().MemberStatus()

	for ep, st := range member {
		cm := meta.VisualIns{
			EndPoint: ep,
			Phase:    st,
		}
		v.ClusterManager = append(v.ClusterManager, cm)
	}

	s, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		log.Warnf("Failed to MarshalIndent %v err %s", v, err.Error())
		return fmt.Sprintf("Failed to MarshalIndent %v err %s", v, err.Error())
	}

	return string(s)
}

func (m *ClusterManager) Version() string {
	ver := common.ClusterManagerVersion{}
	ver.Version = "v1.1.0-" + version.BuildDate + "-" + version.GitCommitId
	pm := plugin.GetPluginManager(nil)
	pm.PluginLock.Lock()

	for name, _ := range pm.PluginsStatus {
		v := common.PluginVersion{
			Name:    name,
			Version: ver.Version,
		}

		ver.Plugins = append(ver.Plugins, v)
	}
	pm.PluginLock.Unlock()

	buf, err := json.MarshalIndent(ver, "", "\t")
	if err != nil {
		return errors.Wrapf(err, "Failed to marshal.").Error()
	}
	return string(buf)
}

func (m *ClusterManager) Status() string {
	phase := m.MetaManager.GetPhase()
	if phase == common.SwitchingPhase {
		return m.StatusManager.Status(m.MetaManager)
	}
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	return m.StatusManager.Status(m.MetaManager)
}

func (m *ClusterManager) RunningFileConf(timestamp time.Time) string {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	s, err := json.MarshalIndent(m.MetaManager.ClusterFileConf, "", "\t")
	if err != nil {
		log.Warnf("Failed to MarshalIndent %v err %s", m.MetaManager.ClusterFileConf, err.Error())
		return fmt.Sprintf("Failed to MarshalIndent %v err %s", m.MetaManager.ClusterFileConf, err.Error())
	}

	return string(s)
}

func (m *ClusterManager) StatusWithoutLock() string {
	return m.StatusManager.Status(m.MetaManager)
}

func (m *ClusterManager) LockReadOnly() error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()
	return m.MetaManager.SetLockReadOnly(true)
}

func (m *ClusterManager) UnlockReadOnly() error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()
	return m.MetaManager.SetLockReadOnly(false)
}

func (m *ClusterManager) DiskExpansion() error {
	return nil
}

func (m *ClusterManager) UpdateConf() error {
	return nil
}

func (m *ClusterManager) StopCluster() error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()
	return m.MetaManager.SetStopped(true)
}

func (m *ClusterManager) StartCluster() error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()
	return m.MetaManager.SetStopped(false)
}

func (m *ClusterManager) AddInsWithoutLock(insSpec *common.InsSpec, engineType common.EngineType) error {
	if err := m.MetaManager.AddInsSpec(insSpec, engineType); err != nil {
		return err
	} else {
		log.Infof("Success add ins spec %s type %s", insSpec.String(), engineType)
	}
	return nil
}

func (m *ClusterManager) AddIns(insSpec *common.InsSpec, engineType common.EngineType) error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	if insSpec.ClusterID == "" {
		if engineType == common.RwEngine || engineType == common.StandbyEngine {
			insSpec.ClusterID = fmt.Sprintf("%x", md5.Sum([]byte(insSpec.Endpoint.Host+":"+insSpec.Endpoint.Port)))
		} else {
			insSpec.ClusterID = m.MetaManager.GetMasterClusterID()
			if insSpec.ClusterID == "" {
				return errors.Errorf("Failed to add ins %s as %s cause rw not exist", insSpec.String(), engineType)
			}
		}
	}

	return m.AddInsWithoutLock(insSpec, engineType)
}

func (m *ClusterManager) AddProxy(spec *common.InsSpec) error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	return m.MetaManager.AddProxySpec(spec)
}

func (m *ClusterManager) RemoveProxy(spec *common.InsSpec) error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	return m.MetaManager.RemoveProxySpec(spec)
}

func (m *ClusterManager) WaitInsReady(spec *common.InsSpec, timeout time.Duration) error {
	return m.MetaManager.WaitInsReady(spec, timeout, &m.ManagerLock)
}

func (m *ClusterManager) WaitInsRemove(spec *common.InsSpec, timeout time.Duration) error {
	return m.MetaManager.WaitInsRemove(spec, timeout, &m.ManagerLock)
}

func (m *ClusterManager) RemoveInsWithoutLock(insSpec *common.InsSpec) error {
	if err := m.MetaManager.RemoveInsSpec(insSpec, true); err != nil {
		return err
	} else {
		log.Infof("Success remove ins spec %s", insSpec.String())
	}
	return nil
}

func (m *ClusterManager) RemoveIns(insSpec *common.InsSpec) error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()
	spec, err := m.MetaManager.GetInsSpec(&insSpec.Endpoint)
	if err != nil {
		log.Warnf("Failed to remove %s get it err %s", insSpec.String(), err.Error())
		return nil
	}

	return m.RemoveInsWithoutLock(spec)
}

func (m *ClusterManager) EnableWithoutLock(enable bool) error {
	var err error

	if resource.IsPolarBoxOneMode() {
		rwSpec, err := m.MetaManager.GetRwSpec(m.MetaManager.GetMasterClusterID())
		if err != nil {
			log.Warnf("Failed to get rw spec for switch phase")
		} else if enable {
			m.StatusManager.EnablePodStatus(rwSpec)
			m.StatusManager.EnableEngineStatus(rwSpec)
		}
	}

	if enable {
		err = m.MetaManager.SwitchPhase(common.RunningPhase)
	} else {
		err = m.MetaManager.SwitchPhase(common.DisableAllPhase)
	}

	if err != nil {
		return err
	}
	*common.EnableAllAction = enable

	return nil
}

func (m *ClusterManager) Enable(enable bool) error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	return m.EnableWithoutLock(enable)
}

func (m *ClusterManager) WaitRwRunningWithLock(timeout time.Duration) error {
	start := time.Now()
	for {
		phase, _ := m.MetaManager.GetRwEnginePhase()
		if phase == common.EnginePhaseRunning {
			return nil
		}
		if time.Since(start) > timeout {
			return errors.Errorf("wait rw running timeout phase %s cost %s", phase, time.Since(start).String())
		}
		// release lock and wait
		m.ManagerLock.Unlock()

		time.Sleep(time.Second)

		// acquire lock and check
		m.ManagerLock.Lock()
	}
}

func (m *ClusterManager) EnableAutoUpdatePXConf(enable bool) error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()
	*common.EnableAutoUpdatePXConf = enable
	log.Infof("Success to set enable_auto_update_px_conf to %v", enable)

	if enable {
		m.UpdatePXConf()
	}

	return nil
}

func (m *ClusterManager) UpdatePXConf() error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	ac := action.GenerateUpdatePXConfigAction(true)
	if err := m.ExecuteAction(ac); err != nil {
		log.Warnf("Failed to call UpdatePXConf")
		return err
	}

	log.Infof("Success to call UpdatePXConf")
	return nil
}

func (m *ClusterManager) ManualSwitch(from common.EndPoint, to common.EndPoint, switchReq common.SwitchoverReq) error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()
	fromClusterID := ""
	toClusterID := ""
	for clusterID, subCluster := range m.MetaManager.ClusterMeta.SubCluster {
		for endpoint, _ := range subCluster.EnginePhase {
			if endpoint == from.String() {
				fromClusterID = clusterID
			}
			if endpoint == to.String() {
				toClusterID = clusterID
			}
		}
	}

	if fromClusterID == "" || toClusterID == "" || from == to {
		return errors.Errorf("Invalid endpoint from:%s from_cluster:%s to:%s to_cluster:%s", from.String(), fromClusterID, to.String(), toClusterID)
	}
	if fromClusterID == toClusterID && fromClusterID == m.MetaManager.ClusterMeta.MasterClusterID {
		if to == m.MetaManager.ClusterMeta.SubCluster[fromClusterID].RwEndpoint {
			return m.WaitRwRunningWithLock(time.Second * 60)
		}
	}

	if from != m.MetaManager.ClusterMeta.SubCluster[fromClusterID].RwEndpoint {
		return errors.Errorf("from endpoint %s is not rw or standby-rw", from.String())
	}

	if phase, exist := m.MetaManager.ClusterMeta.SubCluster[toClusterID].EnginePhase[to.String()]; !exist {
		return errors.Errorf("new rw phase %s is not exist", to.String())
	} else if phase != common.EnginePhaseRunning && switchReq.Force == 0 {
		return errors.Errorf("new rw %s is not in running phase", to.String())
	}

	if state, exist := m.StatusManager.ClusterStatus.SubCluster[toClusterID].EngineStatus[to]; !exist {
		return errors.Errorf("new rw status %s is not exist", to.String())
	} else if state[common.EngineEventCategory].Name != status.EngineStatusAlive && switchReq.Force == 0 {
		return errors.Errorf("new rw %s is not alive", to.String())
	}

	if spec, exist := m.MetaManager.ClusterSpec.SubCluster[toClusterID].InstanceSpec[to.String()]; exist {
		if spec.SwitchPriority == -1 {
			return errors.Errorf("target %s not switchable", spec.String())
		}
	}

	reason := common.HaReason{
		Reason:        "ManualSwitch",
		ActionStartAt: time.Now(),
		Metrics:       map[string]interface{}{},
	}
	if fromClusterID == toClusterID {
		ac, err := action.GenerateReplicaSwitchAction(m.MetaManager, m.StatusManager, reason, true, switchReq.Force != 0, fromClusterID, &to)
		if err != nil {
			return err
		}
		log.Infof("Success to generate action %s by manual switch", ac.String())
		if err = m.ExecuteAction(ac); err != nil {
			m.AlarmActionFailed(ac, err)
			return err
		}
	} else {
		if fromClusterID != m.MetaManager.GetMasterClusterID() {
			return errors.Errorf("from cluster %s/%s is not master-rw", fromClusterID, from.String())
		}
		standbyRw, err := m.MetaManager.GetRwSpec(toClusterID)
		if err != nil {
			return errors.Wrapf(err, "standby rw not exist")
		}
		if to != standbyRw.Endpoint {
			return errors.Errorf("to endpoint %s is not standby-rw", from.String())
		}
		rto := switchReq.Rto
		rpo := switchReq.RpoAcceptableSeconds
		if switchReq.Force == 0 {
			if switchReq.IgnoreRo == 0 {
				for ep, phase := range m.MetaManager.ClusterMeta.SubCluster[fromClusterID].EnginePhase {
					fromRw, err := m.MetaManager.GetRwSpec(fromClusterID)
					if err != nil {
						return errors.Wrapf(err, "from rw not exist")
					}
					if ep != fromRw.Endpoint.String() {
						if phase == common.EnginePhaseRunning || phase == common.EnginePhaseStarting || phase == common.EnginePhaseStopping {
							return errors.Errorf("master cluster ro %s is alive, skip standby switch", ep)
						}
					} else {
						log.Debugf("ins %s phase %s", ep, phase)
					}
				}
			}

			if replayDelay, exist := m.StatusManager.ClusterStatus.SubCluster[toClusterID].
				EngineStatus[to][common.EngineDelayEventCategory].Value[common.EventReplayDelayTime]; !exist {
				return errors.Errorf("standby %s has no replayDelay metrics", to.String())
			} else {
				if switchReq.Rto == 0 {
					rto = *common.StandbyRTO
					if replayDelay.(int) > *common.StandbyRTO {
						return errors.Errorf("standby %s replayDelay %d second, couldn't execute rto=%d switch.",
							to.String(), replayDelay, *common.StandbyRTO)
					}
				} else if replayDelay.(int) > switchReq.Rto {
					return errors.Errorf("standby %s replayDelay %d second, couldn't execute rto=%d switch.",
						to.String(), replayDelay, switchReq.Rto)
				}
				log.Infof("replayDelay=%d rto=%d default_rto=%d", replayDelay, switchReq.Rto, *common.StandbyRTO)
			}
			if writeDelay, exist := m.StatusManager.ClusterStatus.SubCluster[toClusterID].
				EngineStatus[to][common.EngineDelayEventCategory].Value[common.EventWriteDelayTime]; !exist {
				return errors.Errorf("standby %s has no writeDelay metrics", to.String())
			} else {
				if switchReq.RpoAcceptableSeconds == 0 {
					if writeDelay.(int) > *common.StandbyRPO {
						return errors.Errorf("standby %s writeDelay %d second, couldn't execute rpo=0 switch.",
							to.String(), writeDelay)
					}
				} else if writeDelay.(int) > switchReq.RpoAcceptableSeconds+*common.StandbyRPO {
					return errors.Errorf("standby %s writeDelay %d second, couldn't execute rpo=%d switch.",
						to.String(), writeDelay, switchReq.RpoAcceptableSeconds)
				}
				log.Infof("writeDelay=%d rpo=%d default_rpo=%d", writeDelay, switchReq.RpoAcceptableSeconds, *common.StandbyRPO)
			}
		}
		ac, err := action.GenerateStandbySwitchAction(
			m.MetaManager, m.StatusManager, reason, true, switchReq.Force != 0,
			fromClusterID, toClusterID, rpo, rto)
		if err != nil {
			return err
		}
		log.Infof("Success to generate action %s by manual switch", ac.String())
		if err = m.ExecuteAction(ac); err != nil {
			return err
		}
	}

	if *common.EnableAutoUpdatePXConf {
		ac := action.GenerateUpdatePXConfigAction(true)
		if err := m.ExecuteAction(ac); err != nil {
			m.AlarmActionFailed(ac, err)
		}
	}

	return m.WaitRwRunningWithLock(time.Second * 60)
}

func (m *ClusterManager) AddVip(vip, _interface, mask string) error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()
	rwSpec, err := m.MetaManager.GetRwSpec(m.MetaManager.GetMasterClusterID())
	if err != nil {
		return errors.Wrapf(err, "Failed to AddVip")
	}
	err = resource.GetResourceManager().GetVipManager().AddVip(vip, _interface, mask, rwSpec)
	if err != nil {
		return errors.Wrapf(err, "Failed to AddVip")
	}

	return m.MetaManager.AddVip(vip, _interface, mask, rwSpec)
}

func (m *ClusterManager) RemoveVip(vip string) error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()
	err := resource.GetResourceManager().GetVipManager().RemoveVip(vip)
	if err != nil {
		return errors.Wrapf(err, "Failed to RemoveVip")
	}

	return m.MetaManager.RemoveVip(vip)
}

func Add(l, r string) string {
	l_len := len(l)
	r_len := len(r)
	var lv, rv, ev int
	var res string

	i := 0
	ev = 0

	for i < l_len || i < r_len {
		lv = 0
		rv = 0
		if i < l_len {
			lv, _ = strconv.Atoi(string(l[l_len-i-1]))
		}
		if i < r_len {
			rv, _ = strconv.Atoi(string(r[r_len-i-1]))
		}
		res = strconv.Itoa((lv+rv+ev)%10) + res
		ev = (lv + rv + ev) / 10
		i++
	}

	return res
}

func (m *ClusterManager) HaMode(mode string) error {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	if mode == "auto" {
		*common.HaMode = mode
	} else if mode == "manual" {
		*common.HaMode = mode
	} else {
		return errors.Errorf("Unsupport ha mode %s", mode)
	}

	return nil
}

func (m *ClusterManager) GetDecisionPath() (string, error) {
	m.ManagerLock.Lock()
	defer m.ManagerLock.Unlock()

	return decision.GetDecisionRoute().GetDecisionPath()
}
