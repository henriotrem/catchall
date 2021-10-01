package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tidwall/tinybtree"
	"github.com/tidwall/uhatools"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {

	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Specify the address of the main database and the cluster servers")
		os.Exit(1)
	}
	var err error

	fmt.Println("\n# Starting 'CatchAll - Aggregator Service' application")
	fmt.Println("* Application try connection to the master database", os.Args[1])

	database, err := connectDB(os.Args[1])
	if err != nil {
		return
	}

	nCluster := len(os.Args[2:])
	workers := make([]*uhatools.Cluster, nCluster)

	for idx := 0; idx < nCluster; idx++ {

		servers := strings.Split(os.Args[idx+2], ",")

		fmt.Println("* Application try connection to the database cluster", servers)

		workers[idx], err = connectDBCluster(servers)
		if err != nil {
			return
		}
		defer workers[idx].Close()
	}

	runJobs(database, workers)
}

func connectDB(address string) (*mongo.Database, error) {

	clientOptions := options.Client().ApplyURI("mongodb://" + address)
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		return nil, err
	}

	fmt.Println("* Connected to the master DB")

	db := client.Database("catchall")

	return db, nil
}

func connectDBCluster(servers []string) (*uhatools.Cluster, error) {

	cl := uhatools.OpenCluster(uhatools.ClusterOptions{
		InitialServers: servers,
	})

	if err := pingDBCluster(cl); err != nil {
		fmt.Println("* Failed to connect to the DB cluster")
		cl.Close()
		return nil, err
	}

	fmt.Println("* Connected to the DB cluster")
	return cl, nil
}

func pingDBCluster(cl *uhatools.Cluster) error {

	conn := cl.Get()
	defer conn.Close()

	_, err := uhatools.String(conn.Do("PING"))
	if err != nil {
		return err
	}

	return nil
}

func runJobs(database *mongo.Database, workers []*uhatools.Cluster) {

	for period, err := aggregatePeriod(workers); err == nil; {
		fmt.Println("* New period aggregated", period)
		time.Sleep(5 * time.Second)
	}
}

func aggregatePeriod(workers []*uhatools.Cluster) (*tinybtree.BTree, error) {

	return nil, nil
}

func extractPeriod(worker *uhatools.Cluster) (*tinybtree.BTree, error) {

	return nil, nil
}
