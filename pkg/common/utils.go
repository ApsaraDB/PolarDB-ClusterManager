package common

import (
	"github.com/ngaut/log"
	"runtime"
)

func IsLogEventCategory(category EventCategory) bool {
	switch category {
	case StorageUsageEventCategory:
		fallthrough
	case PodEngineEventCategory:
		fallthrough
	case EngineEventCategory:
		return true
	default:
		return false
	}
}

func LogReturnLineWithRet(ret bool) bool {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		log.Infof("function return %v at %s:%d", ret, file, line)
	} else {
		log.Infof("function return %v info miss", ret)
	}
	return ret
}

func IsHaManualMode() bool {
	return *HaMode == "manual"
}
