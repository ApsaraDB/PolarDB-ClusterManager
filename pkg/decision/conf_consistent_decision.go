package decision

import (
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/action"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
)

type ConfConsistentDecision struct {
}

func (*ConfConsistentDecision) Name() string {
	return "ConfConsistentDecision"
}

func (d *ConfConsistentDecision) Evaluate(m *meta.MetaManager, s *status.StatusManager, e *status.StatusEvent) ([]action.ActionExecutor, error) {
	var actions []action.ActionExecutor

	if e.HasTriggerEventCategory(common.EngineConfEventCategory) {
		return actions, nil
	}

	if !*common.EnableAllAction {
		return nil, nil
	}

	var engineType common.EngineType
	if rwSpec, err := m.GetRwSpec(e.ClusterID); err != nil {
		return nil, err
	} else if rwSpec.Endpoint == e.Endpoint {
		if e.ClusterID == m.ClusterMeta.MasterClusterID {
			engineType = common.RwEngine
		} else {
			engineType = common.StandbyEngine
		}
	} else {
		engineType = common.RoEngine
	}

	insSpec, err := m.GetInsSpec(&e.Endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "status event %v", e)
	}

	var settings []common.FileSetting
	if v, exist := e.CurState[common.EngineConfEventCategory].Value[common.EventFileConf]; exist {
		ok := false
		settings, ok = v.([]common.FileSetting)
		if !ok {
			log.Warnf("Failed to covert file conf %v", v)
		}
	} else {
		return actions, nil
	}

	isInit := len(m.ClusterFileConf.Conf) == 0
	fileSettingUpdated := false

	if engineType == common.RwEngine {
		versionConf := common.VersionFileConf{
			Timestamp: e.TimeStamp,
			Conf:      map[string]common.FileSetting{},
		}
		newSettings := map[string]int{}
		nextSettingName := ""
		for i, setting := range settings {
			if i+1 < len(settings) {
				nextSettingName = settings[i+1].Name
			} else {
				nextSettingName = ""
			}
			if setting.Name == "polar_hostid" {
				continue
			}
			newSettings[setting.Name] = 0
			if v, exist := m.ClusterFileConf.Conf[setting.Name]; exist {
				if setting.Applied && v.FileSetting != setting.FileSetting {
					versionConf.Conf[setting.Name] = v
					m.ClusterFileConf.Conf[setting.Name] = setting
				} else {
					// last file setting
					if v.Name != nextSettingName {
						// not applied means inoperative setting
						if !setting.Applied && setting.Error != "" {
							if v.UnAppliedFileSetting != setting.FileSetting {
								v.UnAppliedFileSetting = setting.FileSetting
								m.ClusterFileConf.Conf[setting.Name] = v

								fileSettingUpdated = true
								log.Infof("add unApplied error conf %v", setting)
							}
						} else {
							if v.UnAppliedFileSetting != "" {
								v.UnAppliedFileSetting = ""
								m.ClusterFileConf.Conf[setting.Name] = v

								fileSettingUpdated = true
								log.Infof("remove unApplied error conf %v", setting)
							}
						}
					}
				}

			} else {
				if !isInit {
					versionConf.Conf[setting.Name] = common.FileSetting{
						Name:  setting.Name,
						Reset: true,
					}
				}
				if nextSettingName != "" && setting.Name == nextSettingName && settings[i+1].Applied {
					continue
				}
				m.ClusterFileConf.Conf[setting.Name] = setting
			}
		}

		for name, v := range m.ClusterFileConf.Conf {
			if _, exist := newSettings[name]; !exist {
				versionConf.Conf[name] = v
				delete(m.ClusterFileConf.Conf, name)
			}
		}

		if len(versionConf.Conf) != 0 {
			m.ClusterFileConf.Versions = append(m.ClusterFileConf.Versions, versionConf)
		}

		if len(versionConf.Conf) != 0 || isInit || fileSettingUpdated {
			if resource.IsPolarBoxMode() || resource.IsMpdOperator() {
				actions = append(actions, &action.UpdateRunningConfActionExecutor{
					Timestamp: e.TimeStamp,
				})
			}
		}
	} else if engineType == common.RoEngine && *common.EnableSyncRoConf {
		var updateSettings []common.FileSetting
		newSettings := map[string]common.FileSetting{}
		for _, setting := range settings {
			if setting.Name == "polar_hostid" {
				continue
			}
			newSettings[setting.Name] = setting
			if _, exist := m.ClusterFileConf.Conf[setting.Name]; !exist {
				updateSettings = append(updateSettings, common.FileSetting{
					Name:  setting.Name,
					Reset: true,
				})
			}
		}
		for name, v := range m.ClusterFileConf.Conf {
			if newV, exist := newSettings[name]; exist {
				if newV.FileSetting != v.FileSetting {
					updateSettings = append(updateSettings, v)
				}
			} else {
				updateSettings = append(updateSettings, v)
			}
		}

		if len(updateSettings) != 0 {
			actions = append(actions, &action.UpdateInsConfActionExecutor{
				Ins:      *insSpec,
				Settings: updateSettings,
			})
		}
	}

	return actions, nil
}
