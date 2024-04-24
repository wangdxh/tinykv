package mylog

import (
	"fmt"
	"time"
)

var Level int = LevelBaisc //LevelAppendEntry | LevelBaisc

const (
	LevelBaisc           = 0x1 << 0
	LevelAppendEntry int = 0x1 << 1
	LevelVote            = 0x1 << 2
)

func Printf(level int, format string, a ...interface{}) {
	if Level&level == level {
		str := fmt.Sprintf(format, a...)
		format := "2006-01-02 15:04:05.000"
		fmt.Printf("%s--%s\n", time.Now().Format(format), str)
	}
}
