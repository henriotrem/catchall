package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
	"github.com/tidwall/uhatools"
)

var cl *uhatools.Cluster

func main() {

	var err error
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Specify the address of database cluster servers")
		os.Exit(1)
	}

	fmt.Println("\n# Starting 'CatchAll - Worker Web Server' application")
	fmt.Println("* Application try connection to the database cluster", os.Args[1])

	if cl, err = connectDBCluster(strings.Split(os.Args[1], ",")); err != nil {
		return
	}
	defer cl.Close()

	fmt.Println("* Application starts the web server on port", os.Args[2])
	startWebServer(os.Args[2])
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

// Start Web Server - Expose the in-memory fault tolerant worker database to http calls
func startWebServer(port string) {

	router := mux.NewRouter().StrictSlash(true)

	router.HandleFunc("/events/{domain}/delivered", incrementDelivered).Methods("PUT")
	router.HandleFunc("/events/{domain}/bounced", incrementBounced).Methods("PUT")

	log.Fatal(http.ListenAndServe(":"+port, router))
}

func incrementDelivered(w http.ResponseWriter, r *http.Request) {
	var params = mux.Vars(r)

	conn := cl.Get()
	defer conn.Close()

	_, err := uhatools.String(conn.Do("INCR", params["domain"], "1", "0"))
	if err != nil {
		w.WriteHeader(500)
	}
}

func incrementBounced(w http.ResponseWriter, r *http.Request) {
	var params = mux.Vars(r)

	conn := cl.Get()
	defer conn.Close()

	_, err := uhatools.String(conn.Do("INCR", params["domain"], "0", "1"))
	if err != nil {
		w.WriteHeader(500)
	}
}
