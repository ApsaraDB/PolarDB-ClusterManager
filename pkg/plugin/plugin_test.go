package plugin

import (
	"github.com/prometheus/common/log"
	"net/http"
	"testing"
	"time"
)

func TestGetPluginManager(t *testing.T) {

	var ctrl ServiceCtrl
	ctrl.Init(8000)
	go ctrl.Start()

	pm := GetPluginManager(&ctrl)

	pm.Load("conf")

	param := map[string]interface{}{
		"a": "b",
	}
	pm.Run(param)

	time.Sleep(time.Second)

	c := http.Client{}
	resp, err := c.Get("http://127.0.0.1:8000/test")
	log.Info(resp, err)

	time.Sleep(time.Second)

	pm.Stop()
}
