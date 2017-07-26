package main

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"net/http"
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
	mesosRole             = flag.String("mesos.role", "", "The role of mesos")
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
	blockedTaskGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "esjob",
		Subsystem: "zookeeper",
		Name:      "job_blocked_minutes",
		Help:      "Jobs are blocked in queue, measure in minute"}, []string{"job_name"})
)

func init() {
	prometheus.MustRegister(childrenGauge)
	prometheus.MustRegister(readyGauge)
	prometheus.MustRegister(schedulerRunningTasksGauge)
	prometheus.MustRegister(invalidExecutors)
	prometheus.MustRegister(blockedTaskGauge)
}

func main() {
	flag.Parse()
	if len(*zkQuorum) < 1 {
		log.Fatal("lack param zookeeper.quorum")
	}
	if len(*mesosRole) < 1 {
		log.Fatal("lack param mesos.role")
	}
	connect := connectZk()
	ZkNodeNumber(connect)
	ReadyQueue(connect)
	SchedulerRunning(connect)
	InvalidExecutor(connect)
	QueueRetention(connect)
	// Expose the registered metrics via HTTP.
	http.Handle("/metrics", promhttp.Handler())
	log.Print(http.ListenAndServe(*addr, nil))
}
