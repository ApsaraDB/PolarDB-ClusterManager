package notify

import (
	"encoding/json"
	"fmt"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"sync"
	"time"
)

var GetClusterNameFn func() string = func() string {
	return "NoNameFunction"
}

type RedLineEventSource struct {
	ResourceType string `json:"resourceType,omitempty"`
	InsName      string `json:"insName,omitempty"`
	From         string `json:"from,omitempty"`
	//Role         int `json:"role,omitempty"`
	Ip string `json:"ip,omitempty"`
}

type RedLineEventBody struct {
	EndPoint     common.EndPoint   `json:"endPoint,omitempty"`
	InsName      string            `json:"insName,omitempty"`
	PodName      string            `json:"podName,omitempty"`
	EngineType   common.EngineType `json:"engineType,omitempty"`
	LogicInsName string            `json:"logicInsName,omitempty"`
	StartAt      string            `json:"startAt,omitempty"`
	Describe     string            `json:"describe,omitempty"`
}

type RedLineEventLevel string
type RedLineEventCode string

type RedLineEventV2 struct {
	EventId   string           `json:"eventId,omitempty"`
	EventCode RedLineEventCode `json:"eventCode,omitempty"`

	Source RedLineEventSource `json:"source,omitempty"`
	Level  RedLineEventLevel  `json:"level,omitempty"`
	Time   int64              `json:"time,omitempty"`
	Body   RedLineEventBody   `json:"body,omitempty"`
}

func (r *RedLineEventV2) String() string {
	if r == nil {
		return ""
	}
	bts, err := json.Marshal(r)
	if err != nil {
		//
	}
	return string(bts)
}

//@Deprecated //临时兼容暂时版本，旧redline版本使用
func BuildDBEventPrefixByIns(ins *common.InsSpec, engineType common.EngineType, endPoint common.EndPoint) map[string]interface{} {
	clusterName := GetClusterNameFn()
	return map[string]interface{}{
		"endPoint":     endPoint,
		"timestamp":    time.Now().UnixNano() / 1e6,
		"startAt":      time.Now().Local().Format("2006-01-02 15:04:05.000"),
		"logicInsName": clusterName,
		"insName":      ins.CustID,
		"podName":      ins.PodName,
		"engineType":   engineType,
	}
}

func GetLocalIP() string {
	return LocalIP
}

//event center: doc: https://yuque.antfin-inc.com/aliyuntx/dg1dfo/wgg0os#985tb ,
//share: https://yuque.antfin-inc.com/docs/share/1d964ca6-18cb-4f9b-bf22-147af9bee3dc?# 《事件中心技术方案设计》
func BuildDBEventPrefixByInsV2(ins *common.InsSpec, engineType common.EngineType, endPoint common.EndPoint) RedLineEventV2 {
	endPointStr := fmt.Sprintf("%s:%s", endPoint.Host, endPoint.Port)
	//from := fmt.Sprintf("cluster-manager:%s(%s)", ins.PodName, endPointStr)
	from := "cluster-manager"
	clusterName := GetClusterNameFn()
	return RedLineEventV2{
		EventId:   genRandomEventId(clusterName, endPointStr),
		EventCode: EventCode_Others,
		Source: RedLineEventSource{
			ResourceType: "polardb-o",
			InsName:      clusterName,
			From:         from,
			//Role:         string(engineType),
			Ip: GetLocalIP(),
		},
		Level: EventLevel_INFO,
		Time:  time.Now().UnixNano() / 1e6,
		Body: RedLineEventBody{
			EndPoint:     endPoint,
			InsName:      ins.CustID,
			PodName:      ins.PodName,
			EngineType:   engineType,
			LogicInsName: clusterName,
			StartAt:      time.Now().Local().Format("2006-01-02 15:04:05.000"),
			Describe:     "",
		},
	}
}

var Order int64 = 0
var OrderLock = sync.Mutex{}

func genRandomEventId(insName, endPoint string) string {
	OrderLock.Lock()
	defer OrderLock.Unlock()
	Order++

	timeStr := time.Now().Local().Format("20060102-150405.000")

	//"CM-polar-rwo-f0976o8b6p6-198.19.64.3:5580-2021-03-15 07:49:00.896-2
	return fmt.Sprintf("CM-%s-%s-%s-%d", insName, endPoint, timeStr, Order)
}
