package action

import (
	"github.com/go-pg/pg"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"time"
)

type StopClusterAction struct {
}

func (StopClusterAction) String() string {
	return "StopClusterAction"
}

func (e *StopClusterAction) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	// 1. checkpoint
	masterClusterID := metaManager.GetMasterClusterID()
	rwSpec, err := metaManager.GetRwSpec(masterClusterID)
	if err != nil {
		log.Warnf("Failed to get cluster %s rw spec err=%s", masterClusterID, err.Error())
		return err
	}

	rwPhase, _ := metaManager.GetRwEnginePhase()
	if rwPhase == common.EnginePhaseRunning {
		info := resource.GetClusterManagerConfig()
		db := pg.Connect(&pg.Options{
			Addr:         rwSpec.Endpoint.String(),
			User:         info.Account.AuroraUser,
			Password:     info.Account.AuroraPassword,
			Database:     common.DefaultDatabase,
			PoolSize:     1,
			DialTimeout:  time.Second,
			ReadTimeout:  time.Second * 180,
			WriteTimeout: time.Second * 180,
		})
		defer db.Close()

		sql := `checkpoint`
		_, err = db.Exec(sql)
		if err != nil {
			return errors.Wrapf(err, "Failed to checkpoint on %s", rwSpec.String())
		}
		log.Infof("Success to checkpoint on %s", rwSpec.String())
	}

	// 2. stop ro
	for clusterID, subCluster := range metaManager.ClusterMeta.SubCluster {
		rw, err := metaManager.GetRwSpec(clusterID)
		if err != nil {
			log.Warnf("Failed to get cluster %s rw err=%s", clusterID, err.Error())
			continue
		}
		for ep, phase := range subCluster.EnginePhase {
			if ep != rw.Endpoint.String() {
				if phase != common.EnginePhasePending && phase != common.EnginePhaseStopped {
					// stop ins
					insSpec, err := metaManager.GetInsSpec(common.NewEndPointWithPanic(ep))
					if err != nil {
						log.Warnf("Failed to get ins %s spec err=%s", ep, err.Error())
						continue
					}

					m := resource.GetResourceManager().GetEngineManager(*common.NewEndPointWithPanic(ep))
					if m == nil {
						log.Warnf("Failed to get ins %s engine manager", ep)
						continue
					}

					err = m.Stop(resource.FastStop, 3)
					if err != nil {
						log.Warnf("Failed to stop %s err=%s", insSpec.String(), err.Error())
					}
					metaManager.SetInsPhase(rwSpec, common.EnginePhaseStopping)

					log.Infof("Success to stop ro %s", insSpec.String())
				}
			}
		}
	}

	if rwSpec.ClusterType == common.PaxosCluster {
		// 3. stop standby
		for clusterID, subCluster := range metaManager.ClusterMeta.SubCluster {
			if clusterID == masterClusterID {
				continue
			}
			rw, err := metaManager.GetRwSpec(clusterID)
			if err != nil {
				log.Warnf("Failed to get cluster %s rw err=%s", clusterID, err.Error())
				continue
			}
			for ep, phase := range subCluster.EnginePhase {
				if ep == rw.Endpoint.String() {
					if phase != common.EnginePhasePending && phase != common.EnginePhaseStopped {
						// stop ins
						insSpec, err := metaManager.GetInsSpec(common.NewEndPointWithPanic(ep))
						if err != nil {
							log.Warnf("Failed to get ins %s spec err=%s", ep, err.Error())
							continue
						}

						m := resource.GetResourceManager().GetEngineManager(*common.NewEndPointWithPanic(ep))
						if m == nil {
							log.Warnf("Failed to get ins %s engine manager", ep)
							continue
						}

						err = m.Stop(resource.FastStop, 3)
						if err != nil {
							log.Warnf("Failed to stop %s err=%s", insSpec.String(), err.Error())
						}

						metaManager.SetInsPhase(insSpec, common.EnginePhaseStopping)

						log.Infof("Success to stop standby %s", insSpec.String())
					}
				}
			}
		}

		// 4. stop rw
		if rwPhase != common.EnginePhasePending && rwPhase != common.EnginePhaseStopped {
			m := resource.GetResourceManager().GetEngineManager(rwSpec.Endpoint)
			if m == nil {
				log.Warnf("Failed to get ins %s engine manager", rwSpec.String())
			} else {
				err = m.Stop(resource.FastStop, 3)
				if err != nil {
					log.Warnf("Failed to stop %s err=%s", rwSpec.String(), err.Error())
				}
				metaManager.SetInsPhase(rwSpec, common.EnginePhaseStopping)

				log.Infof("Success to stop rw %s", rwSpec.String())
			}
		}
	} else {
		// 3. stop rw
		if rwPhase != common.EnginePhasePending && rwPhase != common.EnginePhaseStopped {
			m := resource.GetResourceManager().GetEngineManager(rwSpec.Endpoint)
			if m == nil {
				log.Warnf("Failed to get ins %s engine manager", rwSpec.String())
			} else {
				err = m.Stop(resource.FastStop, 3)
				if err != nil {
					log.Warnf("Failed to stop %s err=%s", rwSpec.String(), err.Error())
				}
				metaManager.SetInsPhase(rwSpec, common.EnginePhaseStopping)

				log.Infof("Success to stop rw %s", rwSpec.String())
			}
		}

		// 4. stop standby
		for clusterID, subCluster := range metaManager.ClusterMeta.SubCluster {
			if clusterID == masterClusterID {
				continue
			}
			rw, err := metaManager.GetRwSpec(clusterID)
			if err != nil {
				log.Warnf("Failed to get cluster %s rw err=%s", clusterID, err.Error())
				continue
			}
			for ep, phase := range subCluster.EnginePhase {
				if ep == rw.Endpoint.String() {
					if phase != common.EnginePhasePending && phase != common.EnginePhaseStopped {
						// stop ins
						insSpec, err := metaManager.GetInsSpec(common.NewEndPointWithPanic(ep))
						if err != nil {
							log.Warnf("Failed to get ins %s spec err=%s", ep, err.Error())
							continue
						}

						m := resource.GetResourceManager().GetEngineManager(*common.NewEndPointWithPanic(ep))
						if m == nil {
							log.Warnf("Failed to get ins %s engine manager", ep)
							continue
						}

						err = m.Stop(resource.FastStop, 3)
						if err != nil {
							log.Warnf("Failed to stop %s err=%s", insSpec.String(), err.Error())
						}

						metaManager.SetInsPhase(insSpec, common.EnginePhaseStopping)

						log.Infof("Success to stop standby %s", insSpec.String())
					}
				}
			}
		}
	}
	return nil
}
