package main

import (
	"encoding/json"
	"errors"
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/samuel/go-zookeeper/zk"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

type MetricTarget struct {
	Namespace string
	Node      string
	EventChan <-chan zk.Event
}

var (
	addr                  = flag.String("listen-address", ":8080", "The address to listen on for HTTP requests.")
	zkQuorum              = flag.String("zookeeper.quorum", "", "The zookeeper quorum.")
	mesosNamespace        = flag.String("mesos.namespace", "mesos", "The state url of mesos")
	schedulerHttpEndpoint = flag.String("schduler.httpEndpoint", "127.0.0.1:8899", "The http endpoint of scheduler")
)

var (
	childrenGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "esjob",
		Subsystem: "zookeeper",
		Name:      "nodes",
		Help:      "Children nodes number"}, []string{"type", "name", "execution_type"})

	readyGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "esjob",
		Subsystem: "zookeeper",
		Name:      "state_ready_queue",
		Help:      "ready number"}, []string{"execution_type"})
	schedulerRunningTasksGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "esjob",
		Subsystem: "scheduler",
		Name:      "state_job_running",
		Help:      "Schduler running tasks"}, []string{"execution_type"})
	invalidExecutors = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "esjob",
		Subsystem: "scheduler",
		Name:      "invalid_executors",
		Help:      "Invalid executors which lack config info"}, []string{"executor_id"})
)

func init() {
	prometheus.MustRegister(childrenGauge)
	prometheus.MustRegister(readyGauge)
	prometheus.MustRegister(schedulerRunningTasksGauge)
	prometheus.MustRegister(invalidExecutors)
}

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

func newMap() *map[string]float64 {
	return &map[string]float64{"DEMAON": 0, "TRANSIENT": 0}
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
	return "/elastic-job-cloud/" + target.Namespace + "/" + target.Node
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

func main() {
	flag.Parse()
	if len(*zkQuorum) < 1 {
		log.Fatal("lack param zookeeper.quorum")
	}
	connect := connectZk()
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
	go executor(func() {
		resp, err := http.Get("http://" + *schedulerHttpEndpoint + "/job/tasks/running")
		if err != nil {
			log.Print(err)
			return
		}
		defer resp.Body.Close()
		jsonStr, _ := ioutil.ReadAll(resp.Body)
		var jsonObj interface{}
		json.Unmarshal(jsonStr, &jsonObj)
		var jobMap = map[string]map[string]bool{"DEAMON": make(map[string]bool), "TRANSIENT": make(map[string]bool)}
		for _, task := range jsonObj.([]interface{}) {
			var jobName string
			if metaInfo, ok := task.(map[string]interface{})["metaInfo"]; ok {
				if name, ok := metaInfo.(map[string]interface{})["name"]; ok {
					jobName = name.(string)
				} else {
					continue
				}
			} else {
				continue
			}
			executionType, err := getExecutionType(connect, jobName)
			if err != nil {
				log.Println(err)
				continue
			}
			if executionTypeMap, ok := jobMap[executionType]; ok {
				executionTypeMap[jobName] = true
			} else {
				jobMap[executionType] = map[string]bool{jobName: true}
			}
		}
		schedulerRunningTasksGauge.Reset()
		for executionType, jobs := range jobMap {
			schedulerRunningTasksGauge.WithLabelValues(executionType).Set(float64(len(jobs)))
		}
	})
	go executor(func() {
		master, err := getMesosMaster(connect)
		if err != nil {
			log.Printf("get master info err %v", err)
			return
		}
		resp, err := http.Get("http://" + master + "/state")
		if err != nil {
			log.Print(err)
			return
		}
		defer resp.Body.Close()
		jsonStr, _ := ioutil.ReadAll(resp.Body)
		var state map[string]interface{}
		json.Unmarshal(jsonStr, &state)
		invalidExecutors.Reset()
		if frameworks, ok := state["frameworks"]; ok {
			for _, framework := range frameworks.([]interface{}) {
				if executors, ok := framework.(map[string]interface{})["executors"]; ok {
					for _, executor := range executors.([]interface{}) {
						if id, ok := executor.(map[string]interface{})["executor_id"]; ok {
							idSegments := strings.Split(id.(string), "@-@")
							exists, _, err := connect.Exists(getQualityPath(getTarget("config", "app")) + "/" + idSegments[0])
							if err == nil && !exists {
								invalidExecutors.WithLabelValues(id.(string)).Inc()
							}
						}
					}
				}
			}
		}
	})

	// Expose the registered metrics via HTTP.
	http.Handle("/metrics", promhttp.Handler())
	log.Print(http.ListenAndServe(*addr, nil))
}
