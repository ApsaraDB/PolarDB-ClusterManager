package resource

import (
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
)

type VipManager interface {
	SwitchVip(from *common.InsSpec, to *common.InsSpec) error
	AddVip(vip, _interface, mask string, spec *common.InsSpec) error
	RemoveVip(vip string) error
	GetVipEndpoint(vip string) (common.EndPoint, error)
}

type LinuxVipManager struct {
	Vip2manager  map[string]NetworkManager
	Endpoint2Vip map[string]string
}

func NewLinuxVipManager() *LinuxVipManager {
	return &LinuxVipManager{
		Vip2manager:  make(map[string]NetworkManager),
		Endpoint2Vip: make(map[string]string),
	}
}

func (m *LinuxVipManager) SwitchVip(from *common.InsSpec, to *common.InsSpec) error {
	var vip string
	var exist bool

	if vip, exist = m.Endpoint2Vip[from.Endpoint.String()]; !exist {
		log.Warnf("Failed to switch vip cause %s has no vip", from.String())
		return nil
	}
	if networkManager, exist := m.Vip2manager[vip]; exist {
		if networkManager.Vip() != vip {
			err := errors.Errorf("Failed to get network manager vip %s network vip %s", vip, networkManager.Vip())
			return errors.Wrapf(err, "Failed to switch vip %s from %s to %s", vip, from.String(), to.String())
		}
		networkManager.RemoteUnsetVip()

		newNetworkManager, err := NewSshNetworkManager(
			networkManager.Vip(), networkManager.Interface(), networkManager.Mask(), to.Endpoint)
		if err != nil {
			return errors.Wrapf(err, "Failed to switch vip %s from %s to %s", vip, from.String(), to.String())
		}
		newNetworkManager.RemoteSetVip()

		m.Vip2manager[vip] = newNetworkManager
		delete(m.Endpoint2Vip, from.Endpoint.String())
		m.Endpoint2Vip[to.Endpoint.String()] = vip

		if err != nil {
			return errors.Wrapf(err, "Failed to switch vip %s from %s to %s", vip, from.String(), to.String())
		}
	} else {
		err := errors.Errorf("Failed to get network manager vip %s", vip)
		return errors.Wrapf(err, "Failed to switch vip %s from %s to %s", vip, from.String(), to.String())
	}

	return nil
}

func (m *LinuxVipManager) AddVip(vip, _interface, mask string, spec *common.InsSpec) error {

	if networkManager, exist := m.Vip2manager[vip]; exist {
		if networkManager.EndPoint().Host == spec.Endpoint.Host {
			if isSet, err := networkManager.IsVipSet(); err == nil && isSet {
				log.Debugf("Add vip %s spec %s already exist", vip, spec.String())
				return nil
			} else if err == nil && !isSet {
				return networkManager.RemoteSetVip()
			} else {
				return err
			}
		}
		networkManager.RemoteUnsetVip()
	}
	newNetworkManager, err := NewSshNetworkManager(vip, _interface, mask, spec.Endpoint)
	if err != nil {
		return errors.Wrapf(err, "Failed to add vip %s to %s", vip, spec.String())
	}
	err = newNetworkManager.RemoteSetVip()
	if err != nil {
		return errors.Wrapf(err, "Failed to add vip %s to %s", vip, spec.String())
	}
	m.Vip2manager[vip] = newNetworkManager
	m.Endpoint2Vip[spec.Endpoint.String()] = vip

	return nil
}

func (m *LinuxVipManager) RemoveVip(vip string) error {
	if networkManager, exist := m.Vip2manager[vip]; exist {
		err := networkManager.RemoteUnsetVip()
		if err != nil {
			return errors.Wrapf(err, "Failed to remove vip %s", vip)
		}
		delete(m.Vip2manager, vip)
		for endpoint, _vip := range m.Endpoint2Vip {
			if vip == _vip {
				delete(m.Endpoint2Vip, endpoint)
				break
			}
		}
	}

	return nil
}

func (m *LinuxVipManager) GetNetworkManager(vip string) NetworkManager {
	if networkManager, exist := m.Vip2manager[vip]; exist {
		return networkManager
	} else {
		return nil
	}
}

func (m *LinuxVipManager) GetVipEndpoint(vip string) (common.EndPoint, error) {
	n := m.GetNetworkManager(vip)
	if n != nil {
		return n.EndPoint(), nil
	} else {
		return common.EndPoint{}, errors.New("not found")
	}
}
