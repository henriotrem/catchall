package main

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Book struct {
	ID    primitive.ObjectID `json:"_id,omitempty" bson:"_id,omitempty"`
	Isbn  string             `json:"isbn,omitempty" bson:"isbn,omitempty"`
	Title string             `json:"title,omitempty" bson:"title,omitempty"`
}

var database = connectDB()

func main() {

	createBook()
	getBooks()
}

func createBook() {

	var book = Book{
		Isbn:  "Isbn 1",
		Title: "Titre 1",
	}

	result, err := database.Collection("books").InsertOne(context.TODO(), book)

	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println("Insert : ", result)
}

func getBooks() {

	filter := bson.M{}

	cur, err := database.Collection("books").Find(context.TODO(), filter)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer cur.Close(context.TODO())

	for cur.Next(context.TODO()) {
		var book Book

		if err = cur.Decode(&book); err != nil {
			log.Fatal(err)
		}

		fmt.Println("Book : ", book)
	}
}

func connectDB() *mongo.Database {

	clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")

	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to database!")

	database := client.Database("go_rest_api")

	return database
}
