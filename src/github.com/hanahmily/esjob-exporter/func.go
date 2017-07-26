package main

import (
	"time"
	"log"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
	"errors"
	"sort"
	"encoding/json"
	"strconv"
)

func connectZk() *zk.Conn {
	var servers []string
	if strings.Contains(*zkQuorum, ",") {
		servers = strings.Split(*zkQuorum, ",")
	} else {
		servers = []string{*zkQuorum}
	}
	log.Printf("Connecting... Servers: %v \n", servers)
	c, _, err := zk.Connect(servers, time.Second)
	if err != nil {
		panic(err)
	}
	return c
}

func newMap() *map[string]float64 {
	return &map[string]float64{"DAEMON": 0, "TRANSIENT": 0}
}

func getExecutionType(connect *zk.Conn, node string) (string, error) {
	v, _, _ := connect.Get(getQualityPath(getTarget("config", "job")) + "/" + node)
	var jsonObj map[string]interface{}
	json.Unmarshal(v, &jsonObj)
	if jobExecutionType, ok := jsonObj["jobExecutionType"]; ok {
		return jobExecutionType.(string), nil
	}
	return "", errors.New("Json format error")
}

func getQualityPath(target *MetricTarget) string {
	return "/" + *mesosRole + "/" + target.Namespace + "/" + target.Node
}

func getTarget(namespace string, node string) *MetricTarget {
	return &MetricTarget{namespace, node, nil}
}

func executor(handler func()) {
	for {
		func() {
			defer catch()
			handler()
			time.Sleep(30 * time.Second)
		}()
	}
}

func catch() {
	if err := recover(); err != nil {
		log.Println(err)
		time.Sleep(30 * time.Second)
	}
}

func getMesosMaster(connect *zk.Conn) (string, error) {
	nodes, _, err := connect.Children("/" + *mesosNamespace)
	if err != nil {
		return "", err
	}
	var masters = make([]string, 0)
	for _, node := range nodes {
		if strings.HasPrefix(node, "json.info_") {
			masters = append(masters, node)
		}
	}
	if len(masters) < 1 {
		return "", errors.New("Cannot get mesos master info")
	}
	sort.Strings(masters)
	data, _, err := connect.Get("/" + *mesosNamespace + "/" + masters[0])
	if err != nil {
		return "", err
	}
	var jsonObj map[string]interface{}
	json.Unmarshal(data, &jsonObj)
	address := jsonObj["address"].(map[string]interface{})
	ret := address["ip"].(string) + ":" + strconv.Itoa(int(address["port"].(float64)))
	log.Printf("master :%s", ret)
	return ret, nil
}

