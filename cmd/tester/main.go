package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/mailgun/catchall"
)

func main() {

	wg := &sync.WaitGroup{}
	bus := catchall.SpawnEventPool()
	defer bus.Close()

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go mailgun(i, bus, wg)
	}

	wg.Wait()
}

func mailgun(idx int, bus catchall.EventPool, wg *sync.WaitGroup) {

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

		if idx%50 == 0 && i%1_000 == 0 {
			now := time.Now().UnixNano()
			duration := (now - init) / 1_000
			inserted := int64(i * 50)
			throughput := inserted * 1_000_000 / duration
			fmt.Printf("Inserted: %d | Speed: %d/sec \n", inserted, throughput)
		}

		bus.RecycleEvent(event)
	}

	wg.Done()
}
