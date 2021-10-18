package resource

import (
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"net"
	"strconv"
	"strings"
)

func ConvertToIntIP(ip string) (int, error) {
	ips := strings.Split(ip, ".")
	err := errors.New("Not A IP.")
	if len(ips) != 4 {
		return 0, err
	}
	var intIP int
	for k, v := range ips {
		i, err := strconv.Atoi(v)
		if err != nil || i > 255 {
			return 0, err
		}
		intIP = intIP | i<<uint(8*(3-k))
	}
	return intIP, nil
}

func GenerateSlotName(t common.EngineType, ins *common.InsSpec) string {
	if IsPolarPaasMode() || IsPolarPureMode() {
		if ins.CustID == "0" {
			if t == common.StandbyEngine || t == common.DataMax {
				ipInt, err := ConvertToIntIP(ins.Endpoint.Host)
				if err != nil {
					log.Fatalf("Failed to generate slot name for %s %s err %s", t, ins.String(), err.Error())
				}
				return `standby_` + strconv.Itoa(ipInt) + "_" + ins.Endpoint.Port
			} else if t == common.RoEngine {
				ipInt, err := ConvertToIntIP(ins.Endpoint.Host)
				if err != nil {
					log.Fatalf("Failed to generate slot name for %s %s err %s", t, ins.String(), err.Error())
				}
				return `replica_` + strconv.Itoa(ipInt) + "_" + ins.Endpoint.Port
			} else {
				log.Fatalf("Failed to generate slot name for %s %s with mode %s", t, ins.String(), GetClusterManagerConfig().Mode)
			}
		} else {
			if t == common.StandbyEngine || t == common.DataMax {
				return `standby_` + ins.CustID + "_" + ins.ID
			} else if t == common.RoEngine {
				return `replica_` + ins.CustID + "_" + ins.ID
			} else {
				log.Fatalf("Failed to generate slot name for %s %s with mode %s", t, ins.String(), GetClusterManagerConfig().Mode)
			}
		}
	} else if IsPolarSharedMode() {
		if t == common.RoEngine {
			return `replica_` + ins.CustID + "_" + strings.Split(ins.PodName, "-")[len(strings.Split(ins.PodName, "-"))-1]
		} else if t == common.StandbyEngine {
			return `standby_` + ins.CustID + "_" + strings.Split(ins.PodName, "-")[len(strings.Split(ins.PodName, "-"))-1]
		} else {
			log.Fatalf("Failed to generate slot name for %s %s with mode %s", t, ins.String(), GetClusterManagerConfig().Mode)
		}
	} else {
		log.Fatalf("Failed to generate slot name for %s %s with mode %s", t, ins.String(), GetClusterManagerConfig().Mode)
	}

	return ""
}

func IsApplicationName(ins *common.InsSpec, name string) bool {
	if IsPolarSharedMode() || ins.CustID != "0" {
		s := strings.Split(name, "_")
		if len(s) != 3 {
			return false
		}
		id := s[2]
		t := strings.Split(id, "a")
		if ins.ID == t[0] {
			return true
		}
	} else {
		s := strings.Split(name, "_")
		if len(s) != 3 {
			return false
		}
		ip, err := strconv.Atoi(s[1])
		if err != nil {
			return false
		}
		port := s[2]

		ipStr := fmt.Sprintf("%d.%d.%d.%d",
			byte(ip>>24), byte(ip>>16), byte(ip>>8), byte(ip))

		if ins.Endpoint.Host == ipStr && ins.Endpoint.Port == port {
			return true
		}
	}

	return false
}

func IsLogEventCategory(category common.EventCategory) bool {
	switch category {
	case common.StorageUsageEventCategory:
		fallthrough
	case common.PodEngineEventCategory:
		fallthrough
	case common.EngineEventCategory:
		return true
	default:
		return false
	}
}

func GetLocalServerAddr(useAll bool) string {
	interfaces, err := net.Interfaces()
	if err != nil {
		log.Fatalf("Failed to get net interface %s", err.Error())
	}
	matchInterface := GetClusterManagerConfig().ClientInterface
	if matchInterface == "" && IsPolarBoxMode() {
		// PolarBox的service只能起在bond0即内部网络的网卡上
		matchInterface = "bond0"
	}

	if matchInterface != "" {
		for _, netInterface := range interfaces {
			if netInterface.Name == matchInterface {
				addrs, _ := netInterface.Addrs()
				for _, addr := range addrs {
					log.Debugf("Addr %s:%s", addr.String(), addr.Network())
					if strings.Contains(addr.String(), ".") {
						return strings.Split(addr.String(), "/")[0]
					}
				}
			}
		}
	} else {
		if useAll {
			return ""
		}
		for _, netInterface := range interfaces {
			if netInterface.Name == "docker0" {
				continue
			}
			addrs, _ := netInterface.Addrs()
			for _, addr := range addrs {
				log.Debugf("interface %s Addr %s:%s", netInterface.Name, addr.String(), addr.Network())
				if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
					if ipnet.IP.To4() != nil {
						return strings.Split(addr.String(), "/")[0]
					}
				}
			}
		}
	}

	return ""
}
