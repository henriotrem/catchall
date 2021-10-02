package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/tinybtree"
	"github.com/tidwall/uhatools"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type domain struct {
	name      string
	delivered int64
	bounced   int64
}

func getEpoch(now time.Time) string {
	return strconv.FormatInt(now.Unix()/30, 32)
}

func nextEpoch(current string) string {
	epoch, _ := strconv.ParseInt(current, 32, 64)
	return strconv.FormatInt(epoch+1, 32)
}

func previousEpoch(current string) string {
	epoch, _ := strconv.ParseInt(current, 32, 64)
	return strconv.FormatInt(epoch-1, 32)
}

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

// Connect DB - Connect the master to an in-memory fault tolerant worker database
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

// Connect DB Cluster - Connect the master to an in-memory fault tolerant worker database
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

// Ping DB Cluster - To verify connectivity
func pingDBCluster(cl *uhatools.Cluster) error {

	conn := cl.Get()
	defer conn.Close()

	_, err := uhatools.String(conn.Do("PING"))
	if err != nil {
		return err
	}

	return nil
}

// Run jobs - Periodically run an aggregation of statistics
// TODO - Catch-up the statistics when started and use the extract command
// instead of scan to expire the data on the workers
func runJobs(database *mongo.Database, workers []*uhatools.Cluster) {

	for {
		epoch := previousEpoch(getEpoch(time.Now()))
		_, err := aggregatePeriod(workers, epoch)
		if err != nil {
			return
		}
		time.Sleep(30 * time.Second)
	}
}

// Aggregate Period - Create a single period from different clusters
func aggregatePeriod(workers []*uhatools.Cluster, epoch string) (*tinybtree.BTree, error) {

	aggregatedPeriod := &tinybtree.BTree{}
	parts := make([]int64, len(workers))

	for idx, worker := range workers {

		part, err := scanPeriod(worker, epoch)
		if err != nil {
			return nil, err
		}

		for i, n := 0, 0; i < len(part)/3; i, n = i+1, n+3 {

			var d *domain

			name := part[n]
			delivered, _ := strconv.Atoi(part[n+1])
			bounced, _ := strconv.Atoi(part[n+2])

			v, existed := aggregatedPeriod.Get(name)

			if !existed {
				d = new(domain)
				d.name = name
				d.delivered = 0
				d.bounced = 0
			} else {
				d = v.(*domain)
			}

			d.delivered += int64(delivered)
			d.bounced += int64(bounced)

			parts[idx] += int64(delivered) + int64(bounced)

			aggregatedPeriod.Set(d.name, d)
		}
	}

	fmt.Println("* New period aggregated :", parts)

	return aggregatedPeriod, nil
}

// Get Information - Retrieve cluster period information [current] [last retrieved]
func getInformation(worker *uhatools.Cluster) ([]string, error) {

	conn := worker.Get()
	defer conn.Close()

	resp, err := uhatools.String(conn.Do("DBINFO"))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return strings.Split(resp, " "), nil
}

// Scan Period - Retrieve cluster period statistics
func scanPeriod(worker *uhatools.Cluster, epoch string) ([]string, error) {

	conn := worker.Get()
	defer conn.Close()

	resp, err := uhatools.Strings(conn.Do("SCAN", epoch))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return resp, nil
}

// Extract Period - Retrieve cluster period statistics and expire the previous period
func extractPeriod(worker *uhatools.Cluster, epoch string) ([]string, error) {

	conn := worker.Get()
	defer conn.Close()

	resp, err := uhatools.Strings(conn.Do("EXTRACT", epoch))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return resp, nil
}
