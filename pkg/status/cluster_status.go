package status

import (
	"encoding/json"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"time"
)

type ClusterStatus struct {
	SubCluster map[string]*SubClusterStatus   `json:"sub_cluster"`
	Proxy      map[common.EndPoint]MultiState `json:"proxy"`
	StartAt    time.Time
}

type MultiState map[common.EventCategory]State

type SubClusterStatus struct {
	EngineStatus        map[common.EndPoint]MultiState `json:"engine_status"`
	EngineStatusStartAt map[common.EndPoint]time.Time  `json:"engine_status_start_at"`
	PodStatus           map[common.EndPoint]MultiState `json:"pod_status"`
}

func NewClusterStatus() *ClusterStatus {
	return &ClusterStatus{
		SubCluster: make(map[string]*SubClusterStatus),
		Proxy:      make(map[common.EndPoint]MultiState),
		StartAt:    time.Now(),
	}
}

func (c *ClusterStatus) String(m *meta.MetaManager) string {
	type TmpSubClusterStatus struct {
		EngineStatus        map[string]MultiState `json:"engine_status"`
		PodStatus           map[string]MultiState `json:"pod_status"`
		EngineStatusStartAt map[string]time.Time  `json:"engine_status_start_at"`
	}
	type TmpClusterStatus struct {
		SubCluster map[string]*TmpSubClusterStatus `json:"sub_cluster"`
		Proxy      map[string]MultiState           `json:"proxy"`
		Phase      string                          `json:"phase"`
		Spec       *common.ClusterSpecMeta         `json:"spec,omitempty"`
		Meta       *common.ClusterStatusMeta       `json:"meta,omitempty"`
	}

	tmp := TmpClusterStatus{
		SubCluster: make(map[string]*TmpSubClusterStatus),
		Proxy:      make(map[string]MultiState),
	}
	if m != nil {
		tmp.Phase = m.GetPhase()
		if tmp.Phase == common.SwitchingPhase {
			buf, err := json.MarshalIndent(tmp, "", "\t")
			if err != nil {
				return errors.Wrapf(err, "Failed to marshal.").Error()
			}
			return string(buf)
		}

		tmp.Meta = m.ClusterMeta
		tmp.Spec = m.ClusterSpec
	}
	for id, cluster := range c.SubCluster {
		tmpSubCluster := TmpSubClusterStatus{
			EngineStatus:        make(map[string]MultiState),
			PodStatus:           make(map[string]MultiState),
			EngineStatusStartAt: make(map[string]time.Time),
		}
		for ep, state := range cluster.EngineStatus {
			tmpState := MultiState{}
			for k, v := range state {
				if k != common.EngineConfEventCategory {
					tmpState[k] = v
				}
			}
			tmpSubCluster.EngineStatus[ep.String()] = tmpState
		}
		for ep, state := range cluster.PodStatus {
			tmpSubCluster.PodStatus[ep.String()] = state
		}
		for ep, ts := range cluster.EngineStatusStartAt {
			tmpSubCluster.EngineStatusStartAt[ep.String()] = ts
		}
		tmp.SubCluster[id] = &tmpSubCluster
	}
	for ep, state := range c.Proxy {
		tmp.Proxy[ep.String()] = state
	}

	buf, err := json.MarshalIndent(tmp, "", "\t")
	if err != nil {
		return errors.Wrapf(err, "Failed to marshal.").Error()
	}
	return string(buf)
}
