package action

import (
	"github.com/go-pg/pg"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"strings"
	"sync"
	"time"
)

type StartClusterAction struct {
}

func (StartClusterAction) String() string {
	return "StartClusterAction"
}

func (e *StartClusterAction) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	waitGroup := &sync.WaitGroup{}

	// 1. start rw
	masterClusterID := metaManager.GetMasterClusterID()
	rwSpec, err := metaManager.GetRwSpec(masterClusterID)
	if err != nil {
		log.Warnf("Failed to get cluster %s rw spec err=%s", masterClusterID, err.Error())
		return err
	}
	rwPhase, _ := metaManager.GetRwEnginePhase()

	if rwPhase == common.EnginePhaseStopping || rwPhase == common.EnginePhaseStopped {
		m := resource.GetResourceManager().GetEngineManager(rwSpec.Endpoint)
		if m == nil {
			log.Warnf("Failed to get ins %s engine manager", rwSpec.String())
		} else {
			err = m.Start(3)
			if err != nil {
				return errors.Errorf("Failed to start %s err=%s", rwSpec.String(), err.Error())
			}

			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				metaManager.SetInsPhase(rwSpec, common.EnginePhaseStarting)

				if err = e.WaitInsReady(rwSpec); err != nil {
					log.Warnf("Failed to wait standby %s err=%s", rwSpec.String(), err.Error())
					return
				}

				statusManager.CreateOrUpdateEngineStatus(masterClusterID, rwSpec, common.RwEngine, rwSpec)

				log.Infof("Success to start rw %s", rwSpec.String())
			}()
		}
	}

	// 2. start standby
	for clusterID, subCluster := range metaManager.ClusterMeta.SubCluster {
		if clusterID == masterClusterID {
			continue
		}
		standby, err := metaManager.GetRwSpec(clusterID)
		if err != nil {
			log.Warnf("Failed to get cluster %s standby err=%s", clusterID, err.Error())
			continue
		}
		for ep, phase := range subCluster.EnginePhase {
			if ep == standby.Endpoint.String() {
				if phase == common.EnginePhaseStopping || phase == common.EnginePhaseStopped {
					m := resource.GetResourceManager().GetEngineManager(*common.NewEndPointWithPanic(ep))
					if m == nil {
						log.Warnf("Failed to get ins %s engine manager", ep)
						continue
					}

					err = m.Start(3)
					if err != nil {
						log.Warnf("Failed to start %s err=%s", standby.String(), err.Error())
					}

					waitGroup.Add(1)
					go func() {
						defer waitGroup.Done()
						metaManager.SetInsPhase(standby, common.EnginePhaseStarting)

						if err = e.WaitInsReady(standby); err != nil {
							log.Warnf("Failed to wait standby %s err=%s", standby.String(), err.Error())
							return
						}

						statusManager.CreateOrUpdateEngineStatus(clusterID, standby, common.StandbyEngine, rwSpec)

						log.Infof("Success to start standby %s", standby.String())
					}()

				}
			}
		}
	}

	// 3. start ro
	for clusterID, subCluster := range metaManager.ClusterMeta.SubCluster {
		rw, err := metaManager.GetRwSpec(clusterID)
		if err != nil {
			log.Warnf("Failed to get cluster %s rw err=%s", clusterID, err.Error())
			continue
		}
		for ep, phase := range subCluster.EnginePhase {
			if ep != rw.Endpoint.String() {
				if phase == common.EnginePhaseStopping || phase == common.EnginePhaseStopped {
					// start ins
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

					err = m.Start(3)
					if err != nil {
						log.Warnf("Failed to start %s err=%s", insSpec.String(), err.Error())
					}

					waitGroup.Add(1)
					go func() {
						defer waitGroup.Done()
						metaManager.SetInsPhase(insSpec, common.EnginePhaseStarting)

						if err = e.WaitInsReady(insSpec); err != nil {
							log.Warnf("Failed to wait ro %s err=%s", insSpec.String(), err.Error())
						}

						statusManager.CreateOrUpdateEngineStatus(clusterID, insSpec, common.RoEngine, rw)

						log.Infof("Success to start ro %s", insSpec.String())
					}()

				}

			}
		}
	}

	waitGroup.Wait()

	return nil
}

func (e *StartClusterAction) WaitInsReady(ins *common.InsSpec) error {
	startTs := time.Now()
	c := 0
	errCount := 0
	var err error = nil
	for {
		c++
		db := resource.GetResourceManager().GetEngineConn(ins.Endpoint)
		var id int64
		_, err = db.Query(pg.Scan(&id), common.PolarDBRoHealthCheckSQL)
		if err != nil {
			if strings.Contains(err.Error(), common.EngineStart) {
				errCount = 0
			} else {
				errCount++
			}
			if c%10 == 0 {
				log.Info("ins still not ready", db, err, time.Since(startTs))
			}
			resource.GetResourceManager().ResetEngineConn(ins.Endpoint)
			if errCount > 100 {
				log.Warnf("ins still not ready with err %s more than 10s", err.Error())
				break
			}
		} else {
			break
		}
		if time.Since(startTs).Seconds() > 600 {
			log.Warnf("Failed to wait ins ready")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err != nil {
		log.Infof("Failed to wait ins %s ready cost %s", ins.String(), time.Since(startTs).String())
	} else {
		log.Infof("Success to wait ins %s ready cost %s", ins.String(), time.Since(startTs).String())
	}

	return err
}
