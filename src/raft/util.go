package raft

import (
	"fmt"
	"log"
	"time"
)

type Topic int

const (
	Trace Topic = iota
	Tmp
	Info
	Success
	Notice
	Error
)

var topicMap = map[Topic]string{
	Error:   "Error",
	Notice:  "Notice",
	Success: "Success",
	Info:    "Info",
	Tmp:     "Tmp",
	Trace:   "Trace",
}

// Debugging
const Debug = 1 == 0
const Level = 0

// max log count
var logCountOn = 1 == 0
var logCountLimit = 200

// default level
var DefaultLevel = Trace

// func DPrintf(format string, a ...interface{}) (n int, err error) {
// 	if Debug {
// 		log.Printf(format, a...)
// 	}
// 	return
// }

type Color string

var Reset Color = "\033[0m"
var Red Color = "\033[31m"
var Green Color = "\033[32m"
var Yellow Color = "\033[33m"
var Blue Color = "\033[34m"
var Purple Color = "\033[35m"
var Cyan Color = "\033[36m"
var Gray Color = "\033[37m"
var White Color = "\033[97m"

func TestLogger() {
	DInfo("Info")
	DError("Error")
	DSuccess("Success")
	DNotice("Notice")
}

var count = 0

var logCh = make(chan string, 1000)

func Dlog(format string, color Color, topic Topic, a ...interface{}) {
	if !Debug {
		return
	}
	count += 1
	if logCountOn && count > logCountLimit {
		return
	}
	if topic < DefaultLevel {
		return
	}

	logStr := fmt.Sprintf(format, a...)
	logCh <- string(color) + fmt.Sprintf("%v [%7s] ", time.Now().UnixMilli(), topicMap[topic]) + logStr + string(Reset)
}

func init() {
	go func() {
		for {
			logStr := <-logCh
			log.Println(logStr)
		}
	}()
}

func DTrace(format string, a ...interface{}) {
	Dlog(format, White, Trace, a...)
}

func DInfo(format string, a ...interface{}) {
	Dlog(format, Cyan, Info, a...)
}

func DError(format string, a ...interface{}) {
	Dlog(format, Red, Error, a...)
}

func DNotice(format string, a ...interface{}) {
	Dlog(format, Purple, Notice, a...)
}

func DTmp(format string, a ...interface{}) {
	Dlog(format, Yellow, Tmp, a...)
}

func DSuccess(format string, a ...interface{}) {
	Dlog(format, Green, Success, a...)
}
