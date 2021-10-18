package decision

import (
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"time"
)

type InsChangeDecision struct {
	ClusterSpec *common.ClusterSpecMeta
	ClusterMeta *common.ClusterStatusMeta
	m           *meta.MetaManager
}

func (*InsChangeDecision) Name() string {
	return "InsChangeDecision"
}

func (d *InsChangeDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, e *common.SpecEvent) ([]action.ActionExecutor, error) {
	d.ClusterSpec = m.ClusterSpec
	d.ClusterMeta = m.ClusterMeta

	var actions []action.ActionExecutor

	// 1. add rw action
	newRw := d.GetNewRwSpec()
	if newRw != nil {
		actions = append(actions, &action.AddInsActionExecutor{
			InsSpec:    *newRw,
			RwSpec:     *newRw,
			EngineType: common.RwEngine,
		})
		return actions, nil
	}

	// 2. add standby action
	newStandbys := d.GetNewStandbySpec()
	for _, newStandby := range newStandbys {
		if rwSpec, err := m.GetRwSpec(m.GetMasterClusterID()); err == nil {
			actions = append(actions, &action.AddInsActionExecutor{
				InsSpec:    *newStandby,
				RwSpec:     *rwSpec,
				EngineType: common.StandbyEngine,
			})
			return actions, nil
		}
	}

	// 3. add ro action
	newRos := d.GetNewRoSpec()
	for _, newRo := range newRos {
		if rwSpec, err := m.GetRwSpec(newRo.ClusterID); err == nil {
			actions = append(actions, &action.AddInsActionExecutor{
				InsSpec:    *newRo,
				RwSpec:     *rwSpec,
				EngineType: common.RoEngine,
			})
		}
		if *common.EnableAutoUpdatePXConf {
			ac := action.GenerateUpdatePXConfigAction(false)
			actions = append(actions, ac)
		}
	}

	// 4. remove ro & standby action
	deletedInses := d.GetDeletedStandbyRoSpec()
	for _, deletedIns := range deletedInses {
		if rwSpec, err := m.GetRwSpec(deletedIns.ClusterID); err == nil {
			// standby
			if rwSpec.Endpoint == deletedIns.Endpoint || deletedIns.IsStandby {
				if deletedIns.ClusterID != m.GetMasterClusterID() {
					if masterRw, err := m.GetRwSpec(m.GetMasterClusterID()); err != nil {
						log.Errorf("m.GetRwSpec(m.GetMasterClusterID()) raise panic err: %v", err)
						panic(err)
					} else {
						actions = append(actions, &action.AlarmActionExecutor{
							Alarm: common.AlarmInfo{
								Spec: *deletedIns,
							},
							Op: common.RemoveInsAlarm,
						})
						actions = append(actions, &action.RemoveInsActionExecutor{
							InsSpec:            *deletedIns,
							RwSpec:             *masterRw,
							EngineType:         common.StandbyEngine,
							IsRemoveStatusInfo: true,
						})
					}
				} else {
					err = m.RemoveInsSpec(deletedIns, false)
					log.Warnf("Failed to delete rw %s undo it err %v", deletedIns.String(), err)
				}
			} else {
				actions = append(actions, &action.AlarmActionExecutor{
					Alarm: common.AlarmInfo{
						Spec: *deletedIns,
					},
					Op: common.RemoveInsAlarm,
				})
				actions = append(actions, &action.RemoveInsActionExecutor{
					InsSpec:    *deletedIns,
					RwSpec:     *rwSpec,
					EngineType: common.RoEngine,
				})
				if *common.EnableAutoUpdatePXConf {
					ac := action.GenerateUpdatePXConfigAction(false)
					actions = append(actions, ac)
				}
			}
		} else {
			if deletedIns.IsStandby {
				if masterRw, err := m.GetRwSpec(m.GetMasterClusterID()); err != nil {
					panic(err)
				} else {
					actions = append(actions, &action.AlarmActionExecutor{
						Alarm: common.AlarmInfo{
							Spec: *deletedIns,
						},
						Op: common.RemoveInsAlarm,
					})
					actions = append(actions, &action.RemoveInsActionExecutor{
						InsSpec:    *deletedIns,
						RwSpec:     *masterRw,
						EngineType: common.StandbyEngine,
					})
				}
			} else {
				actions = append(actions, &action.AlarmActionExecutor{
					Alarm: common.AlarmInfo{
						Spec: *deletedIns,
					},
					Op: common.RemoveInsAlarm,
				})
				actions = append(actions, &action.RemoveInsActionExecutor{
					InsSpec:            *deletedIns,
					RwSpec:             *rwSpec,
					EngineType:         common.RoEngine,
					IsRemoveStatusInfo: true,
				})
				if *common.EnableAutoUpdatePXConf {
					ac := action.GenerateUpdatePXConfigAction(false)
					actions = append(actions, ac)
				}
			}
		}
	}

	return actions, nil
}

func (d *InsChangeDecision) GetNewRwSpec() *common.InsSpec {
	for clusterID, subClusterSpec := range d.ClusterSpec.SubCluster {
		for endpoint, insSpec := range subClusterSpec.InstanceSpec {
			if _, exist := d.ClusterMeta.SubCluster[clusterID]; !exist {
				d.ClusterMeta.SubCluster[clusterID] = &common.SubClusterStatusMeta{
					EnginePhase:        make(map[string]string),
					EngineTag:          make(map[string]*common.TagMap),
					EnginePhaseStartAt: make(map[string]time.Time),
					ClusterTag: common.TagMap{
						Tags: make(map[string]string),
					},
				}
				log.Infof("ClusterManager add cluster meta %s", clusterID)
			}

			// make sure rw has been initialize before ro, because ro replication delay depend rw
			if _, exist := d.ClusterMeta.SubCluster[clusterID].EnginePhase[endpoint]; !exist {
				if insSpec.ID == subClusterSpec.InitRwID &&
					d.ClusterSpec.InitMasterClusterID == insSpec.ClusterID &&
					d.ClusterMeta.MasterClusterID == "" {
					return insSpec
				}
			}
		}
	}
	return nil
}

func (d *InsChangeDecision) GetNewRoSpec() []*common.InsSpec {
	var roInsSpec []*common.InsSpec
	for clusterID, subClusterSpec := range d.ClusterSpec.SubCluster {
		for endpoint, insSpec := range subClusterSpec.InstanceSpec {
			if insSpec.IsDeleted {
				continue
			}
			if _, exist := d.ClusterMeta.SubCluster[clusterID].EnginePhase[endpoint]; !exist {
				if insSpec.ID != d.ClusterSpec.SubCluster[clusterID].InitRwID ||
					!d.ClusterMeta.SubCluster[clusterID].RwEndpoint.IsDefault() {
					roInsSpec = append(roInsSpec, insSpec)
				}
				continue
			}

			prev := insSpec.PrevSpec
			if prev != nil && prev.PodName != "" && prev.PodName != insSpec.PodName {
				log.Infof("GetNewRoSpec()[%v] found endpoint[%v] pod changed[%v->%v] ",
					insSpec.ID, insSpec.Endpoint.String(), prev.PodName, insSpec.PodName)
				roInsSpec = append(roInsSpec, insSpec)
			}
		}
	}

	return roInsSpec
}

func (d *InsChangeDecision) GetNewStandbySpec() []*common.InsSpec {
	var standbyInsSpec []*common.InsSpec
	for clusterID, subClusterSpec := range d.ClusterSpec.SubCluster {
		for endpoint, insSpec := range subClusterSpec.InstanceSpec {
			if insSpec.IsDeleted {
				continue
			}
			if _, exist := d.ClusterMeta.SubCluster[clusterID]; !exist {
				d.ClusterMeta.SubCluster[clusterID] = &common.SubClusterStatusMeta{
					EnginePhase:        make(map[string]string),
					EngineTag:          make(map[string]*common.TagMap),
					EnginePhaseStartAt: make(map[string]time.Time),
					ClusterTag: common.TagMap{
						Tags: make(map[string]string),
					},
				}
				log.Infof("ClusterManager add cluster meta %s", clusterID)
			}

			if _, exist := d.ClusterMeta.SubCluster[clusterID].EnginePhase[endpoint]; !exist {
				if insSpec.ID == d.ClusterSpec.SubCluster[clusterID].InitRwID &&
					d.ClusterMeta.MasterClusterID != insSpec.ClusterID {
					standbyInsSpec = append(standbyInsSpec, insSpec)
				}
			}
		}
	}

	return standbyInsSpec
}

func (d *InsChangeDecision) GetDeletedStandbyRoSpec() []*common.InsSpec {
	var insSpeces []*common.InsSpec

	for _, subClusterSpec := range d.ClusterSpec.SubCluster {
		for _, insSpec := range subClusterSpec.InstanceSpec {
			if insSpec.IsDeleted {
				insSpeces = append(insSpeces, insSpec)
			}
		}
	}

	return insSpeces
}
