package resource

import (
	"encoding/json"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type RwType string

const (
	RW     RwType = "rw"
	RO     RwType = "r"
	UNKNOW RwType = "unknow"
)

type StoreManager interface {
	QueryRwType(common.InsSpec) (RwType, error)
	UpdateRwType(common.InsSpec, RwType) (string, error)
	QueryTask(string) error
}

type FakeStore struct {
}

func (p *FakeStore) QueryRwType(ep common.InsSpec) (RwType, error) {
	return UNKNOW, nil
}

func (p *FakeStore) UpdateRwType(ep common.InsSpec, rwType RwType) (string, error) {
	return "", nil
}

func (p *FakeStore) QueryTask(taskID string) error {
	return nil
}

type PolarStore struct {
	Endpoint string
	Uid      string
	PdbName  string
}

func NewPolarStore(ep, uid, pdb string) *PolarStore {
	return &PolarStore{
		Endpoint: ep,
		Uid:      uid,
		PdbName:  pdb,
	}
}

func (p *PolarStore) QueryRwType(ins common.InsSpec) (RwType, error) {
	url := p.Endpoint + "/pbdclient/query?pbdName=" + p.PdbName + "&uid=" + p.Uid + "&hostName=" + ins.HostName
	res, err := http.Get(url)
	if err != nil {
		return UNKNOW, err
	}
	defer res.Body.Close()
	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return UNKNOW, err
	}

	if strings.Contains(string(content), "OK") {
		log.Infof("Success to request %s resp %s", url, string(content))
		if strings.Contains(string(content), "\"rwType\":\"r\"") {
			return RO, nil
		} else {
			return RW, nil
		}
	} else {
		log.Infof("Failed to request %s resp %s", url, string(content))
		return UNKNOW, errors.New(string(content))
	}
}

func (p *PolarStore) UpdateRwType(ins common.InsSpec, rwType RwType) (string, error) {
	log.Infof("UpdateRwType Switch %s to %s", ins.HostName, rwType)

	for checkCnt := 0; ; {
		taskID, err := p.UpdateRwTypeAction(ins, rwType)
		if err == nil {
			return taskID, nil
		}

		if checkCnt > 120 {
			return "", errors.Wrapf(err, "Failed to UpdateRwType %s to rw", ins.String())
		}
		checkCnt++
		time.Sleep(time.Millisecond * 500)
	}
}

func (p *PolarStore) UpdateRwTypeAction(ins common.InsSpec, rwType RwType) (string, error) {
	url := p.Endpoint + "/pbdclient/update?pbdName=" + p.PdbName + "&uid=" + p.Uid + "&hostName=" + ins.HostName + "&rwType=" + string(rwType)
	res, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}

	if strings.Contains(string(content), "OK") {
		log.Infof("Success to request %s resp %s", url, string(content))
		type Data struct {
			TaskID int `json:"taskId"`
		}
		type Resp struct {
			Data Data `json:"data"`
		}
		var resp Resp
		err = json.Unmarshal(content, &resp)
		if err != nil {
			log.Infof("Failed to unmarshal %s err %s", string(content), err)
			return "", errors.New(string(content))
		}
		return strconv.Itoa(resp.Data.TaskID), nil
	} else {
		log.Infof("Failed to request %s resp %s", url, string(content))
		return "", errors.New(string(content))
	}

	return "", nil
}

func (p *PolarStore) QueryTask(taskID string) error {
	for checkCnt := 0; ; {
		err := p.QueryTaskAction(taskID)
		if err == nil {
			return nil
		}
		time.Sleep(time.Millisecond * 500)
		checkCnt++
		if checkCnt > 15 {
			return errors.Wrapf(err, "Failed to QueryTask %s", taskID)
		}
	}
}

func (p *PolarStore) QueryTaskAction(taskID string) error {
	log.Infof("QueryTask %s", taskID)

	url := p.Endpoint + "/task/query?taskId=" + taskID
	res, err := http.Get(url)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	if strings.Contains(string(content), "OK") && strings.Contains(string(content), "\"taskStatus\":2") {
		log.Infof("Success to request %s resp %s", url, string(content))
		return nil
	} else {
		log.Infof("Failed to request %s resp %s", url, string(content))
		return errors.New(string(content))
	}

	return nil
}
