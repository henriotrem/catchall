package main

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func main() {

	var tmp, cluster, clusters string
	var args []string
	var nClusters = 3
	var clusterSize = 3

	if len(os.Args) == 3 {
		nClusters, _ = strconv.Atoi(os.Args[1])
		clusterSize, _ = strconv.Atoi(os.Args[2])
	}

	fmt.Println("\n# Starting 'CatchAll - Simulation' application")

	// Delete existing logs
	os.RemoveAll("clusters")
	fmt.Println("* Previous logs deleted")

	// Create Worker Clusters
	for clIdx := 1; clIdx <= nClusters; clIdx++ {

		// Start Worker Cluster Servers
		fmt.Println("* Application starts the fault tolerant worker cluster :", clIdx)
		for i := 0; i < clusterSize; i++ {
			tmp, args = workerClusterArgs(i, clIdx)
			cluster += tmp
			go exec.Command(createName("worker"), args...).Run()
			fmt.Println("* Worker node", i, "started")

			if i == 0 {
				time.Sleep(5 * time.Second)
			}
		}
		// Start Worker Web Servers
		go exec.Command(createName("worker-api"), createArgs("127.0.0.1:1100%d 2200%d ", clIdx, clIdx)...).Run()
		fmt.Println("* Web worker server", clIdx, "started connected to", cluster)

		clusters, cluster = clusters+cluster, ""
	}

	// Start Master Aggregator
	go exec.Command(createName("master"), createArgs("127.0.0.1:27017"+clusters)...).Run()
	fmt.Println("* Aggregator master service started")

	// Start Master Aggregator
	go exec.Command(createName("master-api"), createArgs("127.0.0.1:27017 8085")...).Run()
	fmt.Println("* Web master server started")
	fmt.Println("\n# READY TO RECEIVE EVENTS")

	time.Sleep(1 * time.Hour)
}

func createName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	} else {
		return "./" + name
	}
}

func createArgs(str string, numbers ...interface{}) []string {
	return strings.Split(fmt.Sprintf(str, numbers...), " ")
}

func workerClusterArgs(node int, clIdx int) (string, []string) {
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
	cluster := separator + "127.0.0.1" + port
	return cluster, args
}
