/*
 * Copyright (c) 2018. Alibaba Cloud, All right reserved.
 * This software is the confidential and proprietary information of Alibaba Cloud ("Confidential Information").
 * You shall not disclose such Confidential Information and shall use it only in accordance with the terms of
 * the license agreement you entered into with Alibaba Cloud.
 */

package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type ServiceCtrl struct {
	route     map[string]func(http.ResponseWriter, *http.Request)
	http      *http.Server
	addr      string
	status    map[string]interface{}
	stopEvent chan bool

	rawPre          map[string]uint64 // raw value last time
	buf             bytes.Buffer
	lastCollectTime time.Time
	mutex           sync.Mutex

	Leader   string
	IsLeader bool
}

type ServiceResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg,omitempty"`
	Data string `json:"data,omitempty"`
}

type Extern struct {
	RouteMap map[string]string      `json:"route_map"`
	Context  map[string]interface{} `json:"context"`
}

func (c *ServiceCtrl) Init(port int) {
	c.route = make(map[string]func(http.ResponseWriter, *http.Request))

	c.http = nil

	localAddr := resource.GetLocalServerAddr(true)

	c.addr = localAddr + ":" + strconv.Itoa(port)
	log.Infof("[ServiceCtrl] addr is %s", c.addr)
	c.Leader = c.addr
	c.status = make(map[string]interface{})
	c.stopEvent = make(chan bool, 1)
}

func (c *ServiceCtrl) GetModuleStatus(name string) []byte {
	// TODO c.status[name] -> []byte
	return nil
}

func (c *ServiceCtrl) GetStatus(w http.ResponseWriter, req *http.Request) {
	var moduleName string
	output := c.GetModuleStatus(moduleName)
	w.Write(output)
}

func (c *ServiceCtrl) AddRoute(url string, cb func(http.ResponseWriter, *http.Request)) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, ok := c.route[url]; !ok {
		c.route[url] = cb
		log.Info("[ServiceCtrl] add route", url)
	} else {
		log.Warn("[ServiceCtrl] has same route, ignore current", url)
	}
}

func (c *ServiceCtrl) ResponseError(rsp http.ResponseWriter, err error) {
	r := ServiceResp{
		Code: 500,
		Msg:  err.Error(),
	}

	v, _ := json.Marshal(r)
	rsp.Write(v)

	log.Info("Failed to service err=", err.Error())
}

func (c *ServiceCtrl) Redirect(rsp http.ResponseWriter, req *http.Request) {
	if req.Header.Get("User-Agent") == "clustermanager" {
		c.ResponseError(rsp, errors.Errorf("redirect loop from %s to %s", c.addr, c.Leader))
		return
	}
	url := "http://" + c.Leader + req.RequestURI
	log.Infof("redirect url: %v requestContent %v", url, req.Body)
	httpReq, err := http.NewRequest(req.Method, url, req.Body)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", "clustermanager")

	httpClient := &http.Client{Timeout: time.Duration(20 * time.Second)}
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		log.Warnf("Failed to redirect to %s err %s", c.Leader, err.Error())
		c.ResponseError(rsp, errors.Wrapf(err, "Failed to redirect to %s", c.Leader))
		return
	}
	defer httpResp.Body.Close()
	data, _ := ioutil.ReadAll(httpResp.Body)
	rsp.Write(data)
}

func (c *ServiceCtrl) RemoveRoute(url string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.route, url)
}

func (c *ServiceCtrl) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Info("[c] received request", req.URL.String())

	c.mutex.Lock()
	if cb, ok := c.route[req.URL.Path]; ok {
		log.Warn("found route for:", req.URL.Path)
		c.mutex.Unlock()
		cb(w, req)
	} else {
		c.mutex.Unlock()
		if c.IsLeader {
			w.Write([]byte("404 Not Found"))
			log.Error("404 Not Found:", req.URL.Path)
		} else {
			c.Redirect(w, req)
		}
	}
}

func (c *ServiceCtrl) Start() error {

	c.http = &http.Server{
		Addr:    c.addr,
		Handler: c,
	}

	if err := c.http.ListenAndServe(); err != nil {
		log.Error("[ServiceCtrl] stopped addr", c.addr, "err", err.Error())
		return err
	}

	return nil
}

func (c *ServiceCtrl) Stop() {
	log.Info("[ServiceCtrl] Stop")
	c.http.Shutdown(context.Background())

	c.stopEvent <- true
}

func (c *ServiceCtrl) LoadCtrlPlugin(mInfo *ModuleInfo, pInfo *PluginInfo) error {
	ext, err := externConfParse(pInfo.Extern)
	if err != nil {
		log.Error("[ServiceCtrl] module extern conf parse failed", pInfo.Name, "path", pInfo.Extern, "err", err.Error())
		return err
	}

	if err = c.registerRoute(ext, mInfo, pInfo); err != nil {
		return err
	}

	pInfo.Ctx = ext.Context
	// TODO pull config center should move to ServiceCtrl
	//if err = c.runCtrlPlugin(ext, mInfo, pInfo); err != nil {
	//	return err
	//}
	return nil
}

func (c *ServiceCtrl) RemoveCtrlPlugin(mInfo *ModuleInfo, pInfo *PluginInfo) error {
	ext, err := externConfParse(pInfo.Extern)
	if err != nil {
		log.Error("[ServiceCtrl] module extern conf parse failed", pInfo.Name, "path", pInfo.Path, "err", err.Error())
		return err
	}

	for url, _ := range ext.RouteMap {
		log.Infof("[ServiceCtrl] remove url %s from ServiceCtrl", url)
		c.RemoveRoute("/" + url)
	}

	return nil
}

func (c *ServiceCtrl) runCtrlPlugin(ext *Extern, mInfo *ModuleInfo, pInfo *PluginInfo) error {
	// initialize control plugin and run it
	ctx := make(map[string]interface{})

	init := mInfo.PluginABI.Init
	initCtx, err := init(ctx)
	if err != nil {
		log.Error("[ServiceCtrl] plugin not running", "name", pInfo.Name, "err", err.Error())
		return err
	}

	mInfo.Contexts.Store(pInfo.Name, initCtx)

	run := mInfo.PluginABI.Run
	go func() {
		err := run(initCtx, nil)
		if err != nil {
			log.Error("[ServiceCtrl] run plugin result", "err", err.Error())
		}
	}()
	return nil
}

func (c *ServiceCtrl) registerRoute(ext *Extern, mInfo *ModuleInfo, pInfo *PluginInfo) error {
	for _, fn := range ext.RouteMap {
		if url, ok := mInfo.Eat[fn]; !ok {
			log.Error("[ServiceCtrl] plugin Route not found", fmt.Sprintf("%s", url), "function", fn, "path", pInfo.Path)
			return fmt.Errorf("plugin route not found, function: %s, path: %s", fn, pInfo.Path)
		}
	}

	for url, fn := range ext.RouteMap {
		log.Info("[ServiceCtrl] add handler", "name", pInfo.Name,
			"detail", fmt.Sprintf("%s@%s:%s ==> %s/%s", fn, pInfo.Path, pInfo.Name, c.addr, url))
		c.AddRoute("/"+url, mInfo.Eat[fn].(func(http.ResponseWriter, *http.Request)))
	}

	return nil
}

func externConfParse(file string) (*Extern, error) {
	var ext Extern
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(content, &ext)
	if err != nil {
		return nil, err
	}
	return &ext, nil
}
