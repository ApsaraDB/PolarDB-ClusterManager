/*
This package provides a portable intaface to SmartClientService model.
SmartClientService can publish/subscribe/unsubscribe messages for all.

protocal:
	head + len + body
	head: 	P->PUSH|F->FIN
	len: 	the length of body
	body: 	actual bytes
The message are allowed to pass binary, and passing to subscribescribers accepted.
*/
package smartclient_service

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/ngaut/log"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
)

const (
	PUSH = 'P'
	FIN  = 'F'

	DefaultBufSize = 32
)

// SmartClientService ...
type SmartClientService struct {
	Socks        []*StreamService
	Mutex        sync.Mutex
	WriteChannel chan *Response
	ReadChannel  chan *Request
	Authaddr     string
	Response     *Response
	WaitGroup    *sync.WaitGroup
	Listener     net.Listener
	ListenAddr   string
}

// NewSmartClientService return new SmartClientService intreface.
func NewSmartClientService(res *Response) *SmartClientService {
	rpc := new(SmartClientService)
	if !rpc.updateTopology(res) {
		log.Warn("Topology is not valid, why is this happend?")
	}
	rpc.Socks = make([]*StreamService, 0, 1000)
	rpc.WriteChannel = make(chan *Response, 1000)
	rpc.ReadChannel = make(chan *Request, 1000)
	rpc.WaitGroup = &sync.WaitGroup{}

	// 启动 SmartClient 服务
	rpc.ListenAddr = "0.0.0.0:" + os.Getenv("smartclient_port")

	go func() {
		for t := range rpc.WriteChannel {
			rpc.Mutex.Lock()
			start := time.Now()
			rpc.WaitGroup.Add(len(rpc.Socks))
			for _, c := range rpc.Socks {
				go func(ss *StreamService, s *SmartClientService) {
					defer rpc.WaitGroup.Done()
					err := ss.Send(s, t, PUSH)
					if err != nil {
						log.Errorf("failed to push topology to client: %s, err: %s", ss.Conn.RemoteAddr().String(), err.Error())
					}
				}(c, rpc)
			}
			rpc.WaitGroup.Wait()
			elapsed := time.Since(start)
			log.Infof("push topology to %d clients cost %s", len(rpc.Socks), elapsed)
			rpc.Mutex.Unlock()
		}
	}()

	return rpc
}

func (s *SmartClientService) Start() {
	go func() {
		log.Infof("Start SmartClient service on %s", s.ListenAddr)
		l, err := net.Listen("tcp", s.ListenAddr)
		if err != nil {
			log.Fatalf("Failed to Listen SmartClient service err %s", err.Error())
			common.StopFlag = true
			return
		}
		s.Listener = l

		for {
			c, err := s.Listener.Accept()
			if common.StopFlag {
				break
			}
			if err != nil {
				log.Errorf("Failed to Accept SmartClient service err %s", err.Error())
				continue
			}

			go func(c net.Conn) {
				ss := &StreamService{
					Conn:       c,
					Reader:     bufio.NewReaderSize(c, DefaultBufSize),
					ReadBytes:  make([]byte, 1024),
					WriteBytes: make([]byte, 5),
				}
				s.subscribe(ss)
				defer func() {
					s.unsubscribe(ss)
					c.Close()
				}()
				firstTime := true
				for {
					req, err := ss.Recv()
					if err != nil {
						break
					}

					if !firstTime {
						if req.Message == "ping" {
							ss.Send(s, &Response{Message: "pong"}, PUSH)
							if err != nil {
								break
							}
						} else {
							s.ReadChannel <- req
						}
					} else {
						firstTime = false
						// auth
						// err = rpc.auth(req)
						// if err != nil {
						// 	ss.Send(rpc, &Response{
						// 		Message: err.Error(),
						// 	}, FIN)
						// 	break
						// }
						err = ss.Send(s, s.Response, PUSH)
						if err != nil {
							break
						}
					}
				}
			}(c)
		}
	}()
}

// Close 客户端关闭
func (rpc *SmartClientService) Stop() {
	close(rpc.ReadChannel)
	close(rpc.WriteChannel)
	rpc.Listener.Close()
}

// StreamService contains connection.
type StreamService struct {
	Conn       net.Conn
	Reader     *bufio.Reader
	ReadBytes  []byte
	WriteBytes []byte
}

// Recv 接收bytes并解析协议
func (ss *StreamService) Recv() (*Request, error) {
	head := ss.ReadBytes[:1]
	n, err := io.ReadFull(ss.Reader, head)
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, fmt.Errorf("protocal error")
	}

	length := ss.ReadBytes[:4]
	n, err = io.ReadFull(ss.Reader, length)
	if err != nil {
		return nil, err
	}
	if n != 4 {
		return nil, fmt.Errorf("protocal error")
	}

	bodylen := int(binary.BigEndian.Uint32(length))
	if bodylen > len(ss.ReadBytes) {
		ss.ReadBytes = make([]byte, bodylen)
	}
	body := ss.ReadBytes[:bodylen]
	n, err = io.ReadFull(ss.Reader, body)
	if err != nil {
		return nil, err
	}
	if n != bodylen {
		return nil, fmt.Errorf("protocal error")
	}

	req := &Request{}
	err = json.Unmarshal(body, req)
	if err != nil {
		return nil, err
	}
	// log.Debugf("receive data from client: %s", req.String())
	return req, nil
}

// Send 发送序列化的协议
func (ss *StreamService) Send(rpc *SmartClientService, res *Response, flag byte) error {
	head := ss.WriteBytes[:1]
	head[0] = flag
	n, err := ss.Conn.Write(head)
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("protocal error")
	}

	body, err := json.Marshal(res)
	if err != nil {
		return err
	}
	length := ss.WriteBytes[:4]
	binary.BigEndian.PutUint32(length, uint32(len(body)))
	n, err = ss.Conn.Write(length)
	if err != nil {
		return err
	}
	if n != 4 {
		return fmt.Errorf("protocal error")
	}

	n, err = ss.Conn.Write(body)
	if err != nil {
		return err
	}
	if n != len(body) {
		return fmt.Errorf("protocal error")
	}
	return nil
}

// 新建连接的鉴权处理
func (rpc *SmartClientService) auth(req *Request) error {
	hostspec := strings.Split(rpc.Authaddr, ":")
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		hostspec[0], hostspec[1], req.GetUsername(), req.GetPassword(), req.GetDatabase())
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		return err
	}
	return nil
}

// updateTopology 更新缓存的拓扑
func (rpc *SmartClientService) updateTopology(res *Response) bool {
	// 从response找出连接地址
	for _, topology := range res.GetTopologyEvent().GetTopology() {
		if topology.GetIsMaster() {
			maxScales := topology.GetMaxscale()
			if len(maxScales) > 0 && len(maxScales[0].GetEndpoint()) > 0 {
				// with maxscale
				rpc.Authaddr = maxScales[0].GetEndpoint()
			} else {
				// without maxscale
				rws := topology.GetRw()
				if len(rws) > 0 && len(rws[0].GetEndpoint()) > 0 {
					rpc.Authaddr = rws[0].GetEndpoint()
				}
			}
			break
		}
	}
	if len(rpc.Authaddr) > 0 && len(strings.Split(rpc.Authaddr, ":")) == 2 {
		rpc.Response = res
		return true
	}
	return false
}

// subscribe 新建连接后保存至订阅列表
func (rpc *SmartClientService) subscribe(c *StreamService) {
	rpc.Mutex.Lock()
	defer rpc.Mutex.Unlock()
	rpc.Socks = append(rpc.Socks, c)
	log.Infof("subscribe client: %s, total amount: %d", c.Conn.RemoteAddr().String(), len(rpc.Socks))
}

// unsubscribe 从订阅列表删除此连接
func (rpc *SmartClientService) unsubscribe(c *StreamService) {
	rpc.Mutex.Lock()
	defer rpc.Mutex.Unlock()

	index := -1
	for i, v := range rpc.Socks {
		if v == c {
			index = i
			break
		}
	}
	if index != -1 {
		rpc.Socks = append(rpc.Socks[:index], rpc.Socks[index+1:]...)
		log.Infof("unsubscribe client: %s, total amount: %d", c.Conn.RemoteAddr().String(), len(rpc.Socks))
	}
}

// Recv 客户端接收
func (rpc *SmartClientService) Recv() *Request {
	return <-rpc.ReadChannel
}

// Send 客户端发送
func (rpc *SmartClientService) Send(res *Response) {
	if res.GetTopologyEvent() != nil {
		rpc.updateTopology(res)
	}

	rpc.WriteChannel <- res
}

// Send 通过API发送
func (rpc *SmartClientService) SendByAPI() {
	rpc.WriteChannel <- rpc.Response
}
