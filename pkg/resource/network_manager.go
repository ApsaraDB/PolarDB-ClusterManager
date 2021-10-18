package resource

import (
	"encoding/json"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"github.com/vishvananda/netlink"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"time"
)

type NetworkManager interface {
	RemoteSetVip() error
	RemoteUnsetVip() error
	IsVipSet() (bool, error)
	Vip() string
	Interface() string
	Mask() string
	EndPoint() common.EndPoint
}

type SshNetworkManager struct {
	vip        string
	_interface string
	mask       string
	endpoint   common.EndPoint
	executor   SshCommandExecutor
}

func NewSshNetworkManager(vip, _interface, mask string, endpoint common.EndPoint) (*SshNetworkManager, error) {
	m := &SshNetworkManager{
		vip:        vip,
		_interface: _interface,
		mask:       mask,
		endpoint:   endpoint,
		executor: SshCommandExecutor{
			Host: endpoint.Host,
		},
	}
	hasSudo := ""
	if *common.WorkUser != "root" {
		hasSudo = "sudo "
	}
	_, _, err := m.executor.Exec([]string{hasSudo + "ifconfig"}, 0, time.Second*10)
	if err != nil {
		log.Warnf("Failed to net ssh network manager check ifconfig err %s", err.Error())
	}
	return m, err
}

func (v *SshNetworkManager) IsVipSet() (bool, error) {
	hasSudo := ""
	if *common.WorkUser != "root" {
		hasSudo = "sudo "
	}
	cmd := []string{hasSudo + "ifconfig " + v._interface + ":" + v.endpoint.Port + " | grep " + v.vip}
	out, _, err := v.executor.Exec(cmd, 0, 10*time.Second)
	if err != nil {
		log.Warnf("Failed to exec %v out %s err %s", cmd, out, err.Error())
		return false, err
	}

	return true, nil
}

func (v *SshNetworkManager) RemoteSetVip() error {
	hasSudo := ""
	if *common.WorkUser != "root" {
		hasSudo = "sudo "
	}
	cmd := []string{hasSudo + "ifconfig " + v._interface + ":" + v.endpoint.Port + " " + v.vip + " netmask " + v.mask + " up"}
	out, _, err := v.executor.Exec(cmd, 0, 10*time.Second)
	if err != nil {
		log.Warnf("Failed to exec %v out %s err %s", cmd, out, err.Error())
		return err
	}

	cmd = []string{hasSudo + "arping -I " + v._interface + " -s " + v.vip + " -b -c 1 -U 0.0.0.0"}
	out, _, err = v.executor.Exec(cmd, 0, 10*time.Second)
	if err != nil {
		log.Warnf("Failed to exec %v out %s err %s", cmd, out, err.Error())
	}

	return err
}

func (v *SshNetworkManager) RemoteUnsetVip() error {
	hasSudo := ""
	if *common.WorkUser != "root" {
		hasSudo = "sudo "
	}
	cmd := []string{hasSudo + "ifconfig " + v._interface + ":" + v.endpoint.Port + " down"}
	out, _, err := v.executor.Exec(cmd, 0, 10*time.Second)
	if err != nil {
		log.Warnf("Failed to exec %v out %s err %s", cmd, out, err.Error())
	}

	return err
}

func (v SshNetworkManager) Vip() string {
	return v.vip
}

func (v SshNetworkManager) Interface() string {
	return v._interface
}

func (v SshNetworkManager) Mask() string {
	return v.mask
}

func (v SshNetworkManager) EndPoint() common.EndPoint {
	return v.endpoint
}

type HostNetworkManager struct {
	Client  EndpointHttpClient
	address *netlink.Addr
	link    netlink.Link
}

func NewRemoteNetworkManager(_address, _interface string) (result *HostNetworkManager, error error) {
	result = &HostNetworkManager{
		Client: EndpointHttpClient{
			Endpoint: []common.EndPoint{*common.NewEndPointWithPanic("127.0.0.1:8800")},
		},
	}

	result.address, error = netlink.ParseAddr(_address + "/32")
	if error != nil {
		error = errors.Wrapf(error, "could not parse address '%s'", _address)
		return
	}

	result.link, error = netlink.LinkByName(_interface)
	if error != nil {
		error = errors.Wrapf(error, "could not get link for interface '%s'", _interface)
		return
	}

	return
}

func NewNetworkManager(_address, _interface string) (result *HostNetworkManager, error error) {
	result = &HostNetworkManager{}

	result.address, error = netlink.ParseAddr(_address + "/32")
	if error != nil {
		error = errors.Wrapf(error, "could not parse address '%s'", _address)
		return
	}

	result.link, error = netlink.LinkByName(_interface)
	if error != nil {
		error = errors.Wrapf(error, "could not get link for interface '%s'", _interface)
		return
	}

	return
}

func (v HostNetworkManager) LocalSetVip() error {
	result, error := v.LocalIsVipSet()
	if error != nil {
		return errors.Wrap(error, "ip check in SetVIP failed")
	}

	// Already set
	if result {
		return nil
	}

	if error = netlink.AddrAdd(v.link, v.address); error != nil {
		return errors.Wrap(error, "could not add ip")
	}

	return nil
}

func (v HostNetworkManager) LocalUnsetVip() error {
	result, error := v.LocalIsVipSet()
	if error != nil {
		return errors.Wrap(error, "ip check in UnsetVIP failed")
	}

	// Nothing to delete
	if !result {
		return nil
	}

	if error = netlink.AddrDel(v.link, v.address); error != nil {
		return errors.Wrap(error, "could not delete ip")
	}

	return nil
}

func (v HostNetworkManager) LocalIsVipSet() (result bool, error error) {
	var addresses []netlink.Addr

	addresses, error = netlink.AddrList(v.link, 0)
	if error != nil {
		error = errors.Wrap(error, "could not list addresses")

		return
	}

	for _, address := range addresses {
		if address.Equal(*v.address) {
			return true, nil
		}
	}

	return false, nil
}

func (v HostNetworkManager) Vip() string {
	return v.address.IP.String()
}

func (v HostNetworkManager) Interface() string {
	return v.link.Attrs().Name
}

func (v *HostNetworkManager) RemoteSetVip() error {
	url := "/v1/set_vip"
	param := map[string]interface{}{
		"interface": v.Interface(),
		"vip":       v.Vip(),
	}

	err, res := v.Client.PostRequest(url, param, false)
	if err != nil {
		log.Warnf("Failed to request %s err:%v res:%s", url, err, res)
		return err
	}

	var apiResp common.ApiResp
	err = json.Unmarshal([]byte(res), &apiResp)
	if err != nil {
		log.Warnf("Failed to Unmarshal res %s err %s", res, err.Error())
		return err
	}

	if apiResp.Code != 200 {
		return errors.New(res)
	}

	return nil
}

func (v *HostNetworkManager) RemoteUnsetVip() error {
	url := "/v1/unset_vip"
	param := map[string]interface{}{
		"interface": v.Interface(),
		"vip":       v.Vip(),
	}

	err, res := v.Client.PostRequest(url, param, false)
	if err != nil {
		log.Warnf("Failed to request %s err:%v res:%s", url, err, res)
		return err
	}

	var apiResp common.ApiResp
	err = json.Unmarshal([]byte(res), &apiResp)
	if err != nil {
		log.Warnf("Failed to Unmarshal res %s err %s", res, err.Error())
		return err
	}

	if apiResp.Code != 200 {
		return errors.New(res)
	}

	return nil
}
