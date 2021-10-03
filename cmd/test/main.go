package main

import (
	"fmt"
	"os/exec"
	"strings"
	"time"
)

func main() {

	nClusters := 3
	clusterSize := 3
	var clusters string
	var args []string

	// Delete existing logs
	exec.Command("rmdir", "clusters").Run()

	// Create Worker Clusters
	for clIdx := 1; clIdx <= nClusters; clIdx++ {

		// Start Worker Cluster Servers
		for i := 0; i < clusterSize; i++ {
			clusters, args = workerClusterArgs(clusters, i, clIdx)
			go exec.Command("worker.exe", args...).Run()

			if i == 0 {
				time.Sleep(5 * time.Second)
			}
		}

		// Start Worker Web Servers
		go exec.Command("worker-api.exe", createArgs("127.0.0.1:1100%d 2200%d ", clIdx, clIdx)...).Run()
	}

	// Start Master Aggregator
	fmt.Println(clusters)
	go exec.Command("master.exe", createArgs("127.0.0.1:27017"+clusters)...).Run()

	// Start Master Aggregator
	go exec.Command("master-api.exe", createArgs("127.0.0.1:27017 8085")...).Run()

	time.Sleep(1 * time.Hour)
}

func createArgs(str string, numbers ...interface{}) []string {
	return strings.Split(fmt.Sprintf(str, numbers...), " ")
}

func workerClusterArgs(clusters string, node int, clIdx int) (string, []string) {
	var args []string
	port := fmt.Sprintf(":110%d%d", node, clIdx)
	separator := ""
	if node == 0 {
		args = createArgs("-d clusters/%d -n 1 -a "+port, clIdx)
		separator = " "
	} else {
		args = createArgs("-d clusters/%d -n %d -a "+port+" -j 127.0.0.1:1100%d", clIdx, node+1, clIdx)
		separator = ","
	}
	clusters += separator + "127.0.0.1" + port
	return clusters, args
}
