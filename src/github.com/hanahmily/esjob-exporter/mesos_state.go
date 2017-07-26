package main

import (
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"strings"
)

func InvalidExecutor(connect *zk.Conn){
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
				name := framework.(map[string]interface{})["name"].(string)
				if name[len("Elastic-Job-Cloud-"):] != *mesosRole {
					continue
				}
				log.Printf("check role %s", name[len("Elastic-Job-Cloud-"):])
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
}
