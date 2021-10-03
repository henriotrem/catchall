package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/mailgun/catchall"
)

const (
	MODE_HTTP  = "http"
	MODE_REDIS = "redis"
)

func main() {

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Specify the type of benchkmark you want to perform [redis,http] [nClusters] [nThreads]")
		os.Exit(1)
	}
	if os.Args[1] != MODE_HTTP && os.Args[1] != MODE_REDIS {
		fmt.Fprintf(os.Stderr, "Use the following modes [http,redis]")
		os.Exit(1)
	}
	fmt.Println("\n# Starting 'CatchAll - Benchmark' application")
	fmt.Println("* Application starting the benchmark using", os.Args[1])
	fmt.Println("* Clusters tested", os.Args[2])

	nClusters, _ := strconv.Atoi(os.Args[2])
	nThreads, _ := strconv.Atoi(os.Args[3])
	wg := &sync.WaitGroup{}
	bus := catchall.SpawnEventPool()
	defer bus.Close()

	for i := 0; i < nThreads; i++ {
		wg.Add(1)
		if os.Args[1] == MODE_HTTP {
			go mailgunHttp(i, nClusters, nThreads, bus, wg)
		} else if os.Args[1] == MODE_REDIS {
			go mailgunRedis(i, nClusters, nThreads, bus, wg)
		}
	}

	wg.Wait()
}

func mailgunHttp(idx int, nClusters int, nThreads int, bus catchall.EventPool, wg *sync.WaitGroup) {

	client := &http.Client{}

	init := time.Now().UnixNano()

	for i := 1; i <= 500_000; i++ {
		event := bus.GetEvent()

		url := "http://127.0.0.1:2200" + strconv.Itoa(i%nClusters+1) + "/events/" + event.Domain + "/" + event.Type
		req, err := http.NewRequest(http.MethodPut, url, nil)
		if err != nil {
			log.Fatalf("Wrong URL: %v\n", err)
		}

		_, err = client.Do(req)
		if err != nil {
			log.Fatalf("Could not INCR: %v\n", err)
		}

		if idx%nThreads == 0 && i%1_000 == 0 {
			printStatistics(nThreads, init, i)
		}

		bus.RecycleEvent(event)
	}

	wg.Done()
}

func mailgunRedis(idx int, nClusters int, nThreads int, bus catchall.EventPool, wg *sync.WaitGroup) {

	c, err := redis.Dial("tcp", ":11001")
	if err != nil {
		log.Fatalf("Could not connect: %v\n", err)
	}
	defer c.Close()

	init := time.Now().UnixNano()

	for i := 1; i <= 500_000; i++ {
		event := bus.GetEvent()

		_, err := c.Do("INCR", event.Domain, event.Type == catchall.TypeDelivered, event.Type == catchall.TypeBounced)
		if err != nil {
			log.Fatalf("could not INCR: %v\n", err)
		}

		if idx%nThreads == 0 && i%1_000 == 0 {
			printStatistics(nThreads, init, i)
		}

		bus.RecycleEvent(event)
	}

	wg.Done()
}

func printStatistics(nThreads int, init int64, i int) {
	now := time.Now().UnixNano()
	duration := (now - init) / 1_000
	inserted := int64(i * nThreads)
	throughput := inserted * 1_000_000 / duration
	fmt.Printf("* Inserted: %d | Speed: %d/sec \n", inserted, throughput)
}
