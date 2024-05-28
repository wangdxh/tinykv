package mylog

import (
	"fmt"
	"time"
)

var Level int = LevelBaisc | LevelCompactSnapshot | LevelTransferLeader //| LevelAppendEntry | LevelTest //LevelAppendEntryDetail //| LevelProposal //| LevelHeartbeat

const (
	LevelBaisc             int = 0x1 << 0
	LevelAppendEntry           = 0x1 << 1
	LevelVote                  = 0x1 << 2
	LevelCompactSnapshot       = 0x1 << 3
	LevelHeartbeat             = 0x1 << 4
	LevelTransferLeader        = 0x1 << 5
	LevelProposal              = 0x1 << 6 // 非常多的打印
	LevelAppendEntryDetail     = 0x1 << 7
	LevelTest                  = 0x1 << 8
)

func Printf(level int, format string, a ...interface{}) {
	if Level&level == level {
		str := fmt.Sprintf(format, a...)
		format := "2006-01-02 15:04:05.000"
		fmt.Printf("%s--%s\n", time.Now().Format(format), str)
	}
}

func Basic(format string, a ...interface{}) {
	Printf(LevelBaisc, format, a...)
}

func GetTimeStr() string {
	format := "2006-01-02 15:04:05.000"
	return fmt.Sprintf("%s", time.Now().Format(format))
}

func GetString(data []byte) string {
	if data == nil {
		return "nil"
	}
	return string(data)
}
