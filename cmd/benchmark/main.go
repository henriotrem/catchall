package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mailgun/catchall"
)

func main() {

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Specify the type of benchkmark you want to perform [clusters] [nThreads] [calls]")
		os.Exit(1)
	}
	fmt.Println("\n# Starting 'CatchAll - Benchmark' application")
	fmt.Println("* Web servers called : ", os.Args[3])

	tCalls, _ := strconv.Atoi(os.Args[1])
	nThreads, _ := strconv.Atoi(os.Args[2])
	endpoints := strings.Split(os.Args[3], ",")

	bus := catchall.SpawnEventPool()
	defer bus.Close()

	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = nThreads

	wg := &sync.WaitGroup{}
	for i := 0; i < nThreads; i++ {
		nCalls := tCalls / nThreads
		if i < tCalls%nThreads {
			nCalls++
		}
		wg.Add(1)
		go httpGun(i, nCalls, nThreads, endpoints, bus, wg)
	}

	wg.Wait()
	fmt.Printf("* Completed: %d \n", tCalls)
}

// Reusing the connection
func httpGun(idx int, calls int, nThreads int, endpoints []string, bus catchall.EventPool, wg *sync.WaitGroup) {

	init := time.Now().UnixNano()
	nEndpoints := len(endpoints)

	for i := 1; i <= calls; i++ {
		event := bus.GetEvent()

		url := endpoints[i%nEndpoints] + "/events/" + event.Domain + "/" + event.Type
		req, err := http.NewRequest(http.MethodPut, url, nil)
		if err != nil {
			log.Fatal(err)
		}

		_, err = http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
		}

		if idx == 0 && i%1_000 == 0 {
			printStatistics(nThreads, init, i)
		}

		bus.RecycleEvent(event)
	}

	wg.Done()
}

func printStatistics(nThreads int, init int64, i int) {
	inserted := int64(i * nThreads)
	throughput := inserted * 1_000_000 / ((time.Now().UnixNano() - init) / 1_000)
	fmt.Printf("* Inserted: %d | Speed: %d/sec \n", inserted, throughput)
}
