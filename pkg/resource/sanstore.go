package resource

import (
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
)

type SanStore struct {
	PvcName string

	RoList []common.EndPoint
	Client EndpointHttpClient
}

func NewSanStore(pvcName string) *SanStore {
	return &SanStore{
		PvcName: pvcName,
		Client: EndpointHttpClient{
			UpdateEndpoint: UpdateK8sOperatorEndpoint,
		},
	}
}

func (s *SanStore) QueryRwType(ins common.InsSpec) (RwType, error) {
	return UNKNOW, nil
}

func (s *SanStore) UpdateRwType(ins common.InsSpec, rwType RwType) (string, error) {
	log.Infof("UpdateRwType Switch %s to %s", ins.String(), rwType)

	if rwType == RO {
		s.RoList = append(s.RoList, ins.Endpoint)
	} else if rwType == RW {
		err := s.SwitchSanStore(ins.Endpoint, s.RoList, s.PvcName)
		s.RoList = s.RoList[0:0]
		if err != nil {
			return "", err
		}
	} else {
		return "", errors.New("no know rwType")
	}

	return "", nil
}

func (s *SanStore) QueryTask(taskID string) error {
	s.RoList = s.RoList[0:0]
	return nil
}

func (s *SanStore) SwitchSanStore(rwEp common.EndPoint, roEps []common.EndPoint, pvcName string) error {
	url := "/api/v1/namespace/" + GetClusterManagerConfig().Cluster.Namespace + "/name/" + GetClusterManagerConfig().Cluster.Name + "/pvc_rwo_relate_set"
	roInsList := ""
	for i, roEp := range roEps {
		if i != 0 {
			roInsList += "|"
		}
		roInsList += roEp.String()
	}
	param := map[string]interface{}{
		"rwInsList": rwEp.String(),
		"roInsList": roInsList,
		"pvcName":   pvcName,
	}

	err, errMsg := s.Client.PostRequest(url, param, true)
	if err != nil {
		log.Warnf("Failed to switch san store err:%v msg:%s req:%s:%v", err, errMsg, url, param)
		return err
	}
	return nil

}
