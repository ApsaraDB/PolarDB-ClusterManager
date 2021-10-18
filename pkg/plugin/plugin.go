/*
 * Copyright (c) 2018. Alibaba Cloud, All right reserved.
 * This software is the confidential and proprietary information of Alibaba Cloud ("Confidential Information").
 * You shall not disclose such Confidential Information and shall use it only in accordance with the terms of
 * the license agreement you entered into with Alibaba Cloud.
 */

package plugin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"os"
	"path"
	"strings"
	"sync"
)

const (
	PluginModeCtrl = "ctrl"

	MaxFailCounts = 30
)

// PluginInfo for plugin
type PluginInfo struct {
	Type           string      `json:"type"`
	Mode           string      `json:"mode"`
	Runner         string      `json:"runner"`
	Exports        []string    `json:"exports"`
	Imports        []string    `json:"imports"`
	Path           string      `json:"path"`
	Name           string      `json:"name"`
	Extern         string      `json:"extern"`
	WorkDir        string      `json:"workdir"`
	WorkDirPrefix  string      `json:"prefix"`
	DoubleCheckDir string      `json:"doulecheckdir"`
	Target         string      `json:"target"`       // 用于判断实例类型
	BusinessType   string      `json:"businessType"` // 用于上报数据的类型
	BizType        string      `json:"biz_type"`     //区分实例是属于公共云还是阿里集团
	Enable         bool        `json:"enable"`
	Interval       int         `json:"interval"`
	Backend        string      `json:"backend"`
	Dependencies   []string    `json:"dependencies"`
	Ctx            interface{} `json:"ctx"`
}

// PluginManager : plugin manager
type PluginManager struct {
	Ctrl          *ServiceCtrl
	Plugins       map[string]*PluginInfo
	Modules       map[string]*ModuleInfo
	PluginsStatus map[string]string
	PluginLock    sync.Mutex
	isloaded      bool
}

var once sync.Once
var pluginManager *PluginManager

func GetPluginManager(ctrl *ServiceCtrl) *PluginManager {
	once.Do(func() {
		pluginManager = &PluginManager{
			Ctrl:          ctrl,
			Plugins:       make(map[string]*PluginInfo),
			Modules:       make(map[string]*ModuleInfo),
			PluginsStatus: make(map[string]string),
			isloaded:      false,
		}
	})

	return pluginManager
}

func (pm *PluginManager) Status(v *meta.Visual) {
	pm.PluginLock.Lock()
	defer pm.PluginLock.Unlock()
	for name, status := range pm.PluginsStatus {
		st := meta.PluginStatus{
			Name:   name,
			Status: status,
		}
		v.Plugins = append(v.Plugins, st)
	}
}

func pluginConfParse(file string, buf *bytes.Buffer) (*PluginInfo, error) {
	var plugin PluginInfo
	plugin.Enable = true
	buf.Reset()
	fp, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	_, err = buf.ReadFrom(fp)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(buf.Bytes(), &plugin)
	if err != nil {
		return nil, err
	}
	return &plugin, nil
}

func (pm *PluginManager) loadPlugin(pInfo *PluginInfo) error {
	// already exists
	if _, ok := pm.Plugins[pInfo.Name]; ok {
		log.Infof("[plugin] skip already exist plugin %s", pInfo.Name)
		return errors.Errorf("plugin %s already exist", pInfo.Name)
	}

	mInfo := new(ModuleInfo)

	// load .so
	err := mInfo.ModuleInit(pInfo)
	if err != nil {
		log.Errorf("[plugin] module %s init failed err %s", pInfo.Name, err.Error())
		return err
	}
	log.Info("[plugin] module init success", pInfo.Name)

	if pInfo.Mode == PluginModeCtrl {
		err = pm.Ctrl.LoadCtrlPlugin(mInfo, pInfo)
		if err != nil {
			log.Warnf("Failed to load ctrl plugin %s err %s", pInfo.Name, err.Error())
			return err
		}
	}

	// initialize control plugin and run it
	ctx, ok := pInfo.Ctx.(map[string]interface{})
	if !ok {
		err = errors.New("ctx should be map[string]interface{}")
		log.Warnf("Failed to init plugin %s err %s", pInfo.Name, err.Error())
		return err
	}

	init := mInfo.PluginABI.Init
	initCtx, err := init(ctx)
	if err != nil {
		log.Warnf("Failed to init plugin %s err %s", pInfo.Name, err.Error())
		return err
	}
	mInfo.Contexts.Store(pInfo.Name, initCtx)

	pm.Plugins[pInfo.Name] = pInfo
	pm.Modules[pInfo.Name] = mInfo

	return nil
}

func (pm *PluginManager) Load(dir string) {
	if pm.isloaded {
		return
	}

	var buf bytes.Buffer
	file, err := os.Open(dir)
	if err != nil {
		log.Errorf("Failed to open dir dir=%s err=%s", dir, err.Error())
		return
	}

	filenames, err := file.Readdirnames(-1)
	file.Close()
	if err != nil {
		log.Errorf("Failed to Readdirnames dir=%s err=%s", dir, err.Error())
		return
	}

	for _, filename := range filenames {
		if !strings.HasPrefix(filename, "plugin_") {
			continue
		}
		if resource.IsCloudOperator() || resource.IsMpdOperator() {
			continue
		}
		filePath := path.Join(dir, filename)
		// parse pluginInfo conf
		pluginInfo, err := pluginConfParse(filePath, &buf)
		if err != nil {
			log.Errorf("Failed to parse pluginInfo conf %s err %s", filePath, err.Error())
			continue
		}

		err = pm.loadPlugin(pluginInfo)
		if err != nil {
			log.Warnf("Failed to load plugin %s err %s skip it", pluginInfo.Name, err.Error())
			pm.PluginLock.Lock()
			pm.PluginsStatus[pluginInfo.Name] = fmt.Sprintf("Plugin load err %s", err.Error())
			pm.PluginLock.Unlock()
			continue
		}

		pm.PluginLock.Lock()
		pm.PluginsStatus[pluginInfo.Name] = fmt.Sprintf("Plugin running")
		pm.PluginLock.Unlock()

		log.Infof("load pluginInfo name %s", pluginInfo.Name)
	}

	pm.isloaded = true
}

func (pm *PluginManager) Run(param interface{}) error {

	for name, module := range pm.Modules {
		initCtx, ok := module.Contexts.Load(name)
		if !ok {
			log.Warnf("Failed to load plugin %s init context skip it", name)
			pm.PluginLock.Lock()
			pm.PluginsStatus[name] = fmt.Sprintf("Plugin init ctx fail")
			pm.PluginLock.Unlock()
			continue
		}

		go func() {
			defer func() {
				if err := recover(); err != nil {
					log.Warnf("Failed to run plugin %s crash err %v errCount %d", name, err, module.errCount)
					module.errCount++
				}
			}()
			st := ""
			err := module.PluginABI.Run(initCtx, param)
			if err != nil {
				module.errCount++
				st = fmt.Sprintf("Plugin run err %s errCount %d", err.Error(), module.errCount)

				log.Warnf("Failed to run plugin %s err %s errCount %d", name, err.Error(), module.errCount)
			} else {
				module.errCount = 0
				st = fmt.Sprintf("Plugin running")

				log.Infof("Success to run plugin %s with param %v", name, param)
			}

			pm.PluginLock.Lock()
			pm.PluginsStatus[name] = st
			pm.PluginLock.Unlock()
		}()
	}

	return nil
}

func (pm *PluginManager) Stop() {

	for name, module := range pm.Modules {
		initCtx, ok := module.Contexts.Load(name)
		if !ok {
			log.Warnf("Failed to load plugin %s init context skip it", name)
			continue
		}

		err := module.PluginABI.Exit(initCtx)
		if err != nil {
			log.Warnf("Failed to exit plugin %s err %s errCount %d", name, err.Error(), module.errCount)
			continue
		}

		pInfo := pm.Plugins[name]

		if pm.Plugins[name].Mode == PluginModeCtrl {
			err = pm.Ctrl.RemoveCtrlPlugin(module, pInfo)
			if err != nil {
				log.Warnf("Failed to load ctrl plugin %s err %s", pInfo.Name, err.Error())
				continue
			}
		}
	}

	pm.Plugins = make(map[string]*PluginInfo)
	pm.Modules = make(map[string]*ModuleInfo)

	pm.isloaded = false
}
