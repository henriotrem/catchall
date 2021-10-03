package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var database *mongo.Database

type domain struct {
	ID        primitive.ObjectID `json:"-"      bson:"_id,omitempty"`
	Name      string             `json:"-"      bson:"name"`
	Delivered int                `json:"-"      bson:"delivered"`
	Bounced   int                `json:"-"      bson:"bounced"`
	Status    string             `json:"status" bson:"-"`
}

const (
	UNKNOWN_STATUS     = "Unknown"
	CATCHALL_STATUS    = "CatchAll"
	NONCATCHALL_STATUS = "NonCatchAll"
)

func main() {

	var err error
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Specify the address of database cluster servers")
		os.Exit(1)
	}

	fmt.Println("\n# Starting 'CatchAll - Master Web Server' application")
	fmt.Println("* Application try connection to the master database", os.Args[2])

	database, err = connectDB(os.Args[2])
	if err != nil {
		return
	}

	fmt.Println("* Application starts the web server on port", os.Args[1])
	startWebServer(os.Args[1])
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

// Start Web Server - Expose the master database to http calls
func startWebServer(port string) {

	router := mux.NewRouter().StrictSlash(true)

	router.HandleFunc("/domains/{name}", getDomain).Methods("GET")

	log.Fatal(http.ListenAndServe(":"+port, router))
}

func getDomain(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var params = mux.Vars(r)
	d := &domain{}

	err := d.get(params["name"])
	if err != nil {
		w.WriteHeader(404)
	}

	json.NewEncoder(w).Encode(d)
}
func (domain *domain) get(name string) error {
	var ctx context.Context

	query := bson.M{"name": name}
	err := database.Collection("domains").FindOne(ctx, query).Decode(&domain)

	if domain.Bounced > 0 {
		domain.Status = NONCATCHALL_STATUS
	} else if domain.Delivered < 1000 {
		domain.Status = UNKNOWN_STATUS
	} else {
		domain.Status = CATCHALL_STATUS
	}

	return err
}
