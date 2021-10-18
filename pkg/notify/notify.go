package notify

import (
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	syslog "log"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	FIXED_YES = "yes"
	FIXED_NO  = "no"
)

type MsgNotifyManager struct {
	Log         *syslog.Logger
	fd          *os.File
	file        string
	logSuffix   string
	MsgChanel   chan *MsgInfo
	MsgChanelV2 chan *RedLineEventV2

	redLineClient  *RedLineClient
	polarboxModeFn func() bool
	clusterPortFn  func() int
}

type MsgInfo struct {
	msgId   string
	MsgHead map[string]interface{}
	MsgBody map[string]interface{}
}

var _msgId int64 = 0
var _msgLock = sync.Mutex{}
var LocalIP string

var MsgNotify = NewMsgNotifyManager()

func NewMsgNotifyManager() *MsgNotifyManager {
	return &MsgNotifyManager{
		MsgChanel:     make(chan *MsgInfo, 1000),
		MsgChanelV2:   make(chan *RedLineEventV2, 1000),
		redLineClient: NewRedLineClient(),
	}
}

func NewMsg() *MsgInfo {
	_msgLock.Lock()
	defer _msgLock.Unlock()

	_msgId++

	return &MsgInfo{
		msgId: fmt.Sprintf("%s_%v", time.Now().Local().Format("20060102150405"), _msgId),
	}
}

func (m MsgInfo) String() string {
	if m.MsgHead != nil && len(m.MsgHead) > 0 {
		return fmt.Sprintf("MsgInfo{Id=%s, head:%v, body: %v}", m.msgId, m.MsgHead, m.MsgBody)
	}
	return fmt.Sprintf("MsgInfo{Id=%s, body: %v}", m.msgId, m.MsgBody)
}

func (n *MsgNotifyManager) SendMsgTo(msg *MsgInfo) error {

	if msg == nil {
		return nil
	}
	head := common.GetCallerInfo()

	log.Infof("[%s] put a msg to notify: %s", head, msg.String())

	chanelLen := len(n.MsgChanel)
	if chanelLen > 900 {
		return fmt.Errorf("chanel is full (len=%v, maxLen=1000, stop=900)", chanelLen)
	}
	if n.MsgChanel != nil {
		n.MsgChanel <- msg
	}
	return nil
}

func (n *MsgNotifyManager) SendMsgToV2(msg *RedLineEventV2) error {
	if msg == nil {
		return nil
	}

	chanelLen := len(n.MsgChanelV2)
	if chanelLen > 900 {
		return fmt.Errorf("chanel MsgChanelV2 is full (len=%v, maxLen=1000, stop=900)", chanelLen)
	}
	n.MsgChanelV2 <- msg
	return nil
}

func (n *MsgNotifyManager) Start(ip string, _polarboxModeFn func() bool, _clusterPortFn func() int) {
	log.Infof("MsgNotifyManager started!!")
	LocalIP = ip
	n.polarboxModeFn = _polarboxModeFn
	n.clusterPortFn = _clusterPortFn
	//go n.startV1()
	go n.startV2()
}

func (n *MsgNotifyManager) startV1() {
	for msg := range n.MsgChanel {
		if msg == nil {
			continue
		}
		if n.polarboxModeFn() {
			n.sendMsgToRedLine(msg)
		}

		if *common.EnableEventLog {
			n.LogMsg(msg.MsgBody)
		}
	}
}

func (n *MsgNotifyManager) rotate() error {
	suffix := time.Now().Format("20060102")
	if n.logSuffix == "" {
		n.logSuffix = suffix
	}
	if n.logSuffix == suffix {
		return nil
	}

	lastFileName := n.file + "." + n.logSuffix
	err := os.Rename(n.file, lastFileName)
	if err != nil {
		return errors.Wrapf(err, "Failed to rename from %s to %s", n.file, lastFileName)
	}
	n.fd.Close()

	n.fd, err = os.OpenFile(n.file, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("Failed to open file %s err %s")
	}

	n.Log.SetOutput(n.fd)

	n.logSuffix = suffix

	return nil
}

func (n *MsgNotifyManager) LogMsg(msg interface{}) {
	if msg == nil {
		return
	}

	if n.Log == nil {
		path := *common.EventLogPathPrefix + "/" + strconv.Itoa(n.clusterPortFn())
		os.MkdirAll(path, 0666)
		var err error
		n.file = path + "/cm.event.log"
		n.fd, err = os.OpenFile(n.file, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			log.Fatalf("Failed to open file %s err %s", n.file, err.Error())
		}

		n.Log = syslog.New(n.fd, "", 0)
	}

	err := n.rotate()
	if err != nil {
		log.Warnf("Failed to rotate file %s err %s", n.file, err.Error())
		return
	}

	data, err := json.Marshal(msg)
	if err != nil {
		log.Warnf("Failed to marshal %v err %s", msg, err.Error())
		return
	}

	n.Log.Output(4, string(data))
}

func (n *MsgNotifyManager) startV2() {
	for msg := range n.MsgChanelV2 {
		if msg == nil {
			continue
		}
		if n.polarboxModeFn() {
			n.sendMsgToRedLineV2(msg)
		}

		if *common.EnableEventLog {
			n.LogMsg(msg.Body)
		}
	}
}

func (n *MsgNotifyManager) sendMsgToRedLine(info *MsgInfo) {
	if info == nil {
		return
	}
	log.Infof("Send a msg to redLine: %+v", info.String())
	if info.MsgBody == nil {
		return
	}
	now := time.Now()
	defer func() {
		end := time.Now()
		log.Infof("Send a msg to redLine done : %+v, spend: %v", info.String(), end.Sub(now).Seconds())

	}()

	err := n.redLineClient.SendBoxInsEvent(info.MsgBody)
	if err != nil {
		//
		log.Errorf("Send a msg to redLine: %+v, err: %v", info.String(), err)
	}
}

func (n *MsgNotifyManager) sendMsgToRedLineV2(info *RedLineEventV2) {
	if info == nil {
		return
	}
	/*
		log.Infof("Send a msg to redLineV2: %+v", info.String())

		now := time.Now()
		defer func() {
			end := time.Now()
			log.Infof("Send a msg to redLineV2 done : %+v, spend: %v", info.String(), end.Sub(now).Seconds())
		}()
	*/

	//https://yuque.antfin-inc.com/aliyuntx/dg1dfo/wgg0os#985tb
	// ${insName};${ip};

	idKey := fmt.Sprintf("%s;%s", info.Source.InsName, info.Body.EndPoint.Host)
	err := n.redLineClient.SendBoxInsEventV2(info, idKey)
	if err != nil {
		//
		log.Errorf("Send a msg to redLineV2: %+v, err: %v", info.String(), err)
	}
}

func (n *MsgNotifyManager) Close() {
	log.Infof("MsgNotifyManager close!!")
	close(n.MsgChanel)
	close(n.MsgChanelV2)
}
