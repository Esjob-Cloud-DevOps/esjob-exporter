package main

import (
	"reflect"
	"log"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
)

func metricChildren(connect *zk.Conn, target *MetricTarget) <-chan zk.Event {
	nodes, _, ch, _ := connect.ChildrenW(getQualityPath(target))
	var metricMap = *newMap()
	if strings.EqualFold(target.Namespace, "state") || strings.EqualFold(target.Namespace, "config") && strings.EqualFold(target.Node, "job") {
		for _, node := range nodes {
			jobExecutionType, err := getExecutionType(connect, node)
			if err != nil {
				log.Println(err)
				continue
			}
			metricMap[jobExecutionType] += 1
		}
		for jobExecutionType, number := range metricMap {
			childrenGauge.WithLabelValues(target.Namespace, target.Node, jobExecutionType).Set(number)
		}
	} else {
		childrenGauge.WithLabelValues(target.Namespace, target.Node, "").Set(float64(len(nodes)))
	}

	return ch
}

func ZkNodeNumber(connect *zk.Conn) {
	targets := [...]*MetricTarget{getTarget("config", "app"), getTarget("config", "job"), getTarget("state", "ready"),
								  getTarget("state", "running"), getTarget("state", "failover")}
	var selectCase = make([]reflect.SelectCase, len(targets))
	for i := 0; i < len(targets); i++ {
		target := targets[i]
		target.EventChan = metricChildren(connect, target)
		selectCase[i].Dir = reflect.SelectRecv
		selectCase[i].Chan = reflect.ValueOf(target.EventChan)
	}
	go executor(func() {
		chosen, event, ok := reflect.Select(selectCase)
		if !ok {
			return
		}
		log.Printf("Event:%v", event)
		target := targets[chosen]
		ch := metricChildren(connect, target)
		target.EventChan = ch
		selectCase[chosen].Chan = reflect.ValueOf(ch)
	})
}
