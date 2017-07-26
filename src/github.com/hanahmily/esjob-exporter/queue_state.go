package main

import (
	"log"
	"strconv"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

func ReadyQueue(connect  *zk.Conn) {
	go executor(func() {
		qualityPath := getQualityPath(getTarget("state", "ready"))
		nodes, _, _ := connect.Children(qualityPath)
		var readyMap = *newMap()
		for _, readyChildNode := range nodes {
			v, _, err := connect.Get(qualityPath + "/" + readyChildNode)
			if err != nil {
				log.Print(err)
				continue
			}
			readyNumber, err := strconv.ParseFloat(string(v[:]), 64)
			if err != nil {
				log.Print(err)
				continue
			}
			executionType, err := getExecutionType(connect, readyChildNode)
			if err != nil {
				log.Print(err)
				continue
			}
			readyMap[executionType] += readyNumber
		}
		readyGauge.Reset()
		for executionType, number := range readyMap {
			readyGauge.WithLabelValues(executionType).Set(number)
		}
	})
}

func QueueRetention(connect  *zk.Conn) {
	go executor(func() {
		qualityPath := getQualityPath(getTarget("state", "ready"))
		nodes, _, _ := connect.Children(qualityPath)
		blockedTaskGauge.Reset()
		for _, readyChildNode := range nodes {
			_, stat, err := connect.Get(qualityPath + "/" + readyChildNode)
			if err != nil {
				log.Print(err)
				continue
			}
			retention := time.Now().Sub(time.Unix(0, stat.Ctime * int64(time.Millisecond) / int64(time.Nanosecond))).Minutes()
			if retention > 5 {
				blockedTaskGauge.WithLabelValues(readyChildNode).Set(retention)
			}
		}
	})
}
