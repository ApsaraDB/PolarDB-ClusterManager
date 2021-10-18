package common

import (
	"fmt"
	"runtime"
)

func GetCallerInfo() string {
	_, fileName, line, ok := runtime.Caller(2)
	if !ok {
		fileName = "???"
		line = 0
	}
	short := fileName
	for i := len(fileName) - 1; i > 0; i-- {
		if fileName[i] == '/' {
			short = fileName[i+1:]
			break
		}
	}
	return fmt.Sprintf("%v:%v", short, line)
}
