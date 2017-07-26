package main

import (
	"github.com/samuel/go-zookeeper/zk"
	"net/http"
	"log"
	"io/ioutil"
	"encoding/json"
)

func SchedulerRunning(connect *zk.Conn) {
	go executor(func() {
		resp, err := http.Get("http://ops.cluster/" + *mesosRole + "/api/job/tasks/running")
		if err != nil {
			log.Print(err)
			return
		}
		defer resp.Body.Close()
		jsonStr, _ := ioutil.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			log.Printf("get scheduler api error: %s", jsonStr)
			return
		}
		var jsonObj interface{}
		json.Unmarshal(jsonStr, &jsonObj)
		var jobMap = map[string]map[string]bool{"DAEMON": make(map[string]bool), "TRANSIENT": make(map[string]bool)}
		for _, task := range jsonObj.([]interface{}) {
			var jobName string
			if metaInfo, ok := task.(map[string]interface{})["metaInfo"]; ok {
				if name, ok := metaInfo.(map[string]interface{})["jobName"]; ok {
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
}

