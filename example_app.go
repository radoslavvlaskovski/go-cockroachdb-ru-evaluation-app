package main

import (
	"context"
	"log"
	"os"
	"sync"

	b "cockroachdb/example_app/business"

	"github.com/jackc/pgx/v4"
)

var wg sync.WaitGroup

func executeRequests() {
	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), os.Getenv("COCKROACH_DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(context.Background())

	b.GetDucks(conn, 1000)
}

func main() {

	routineCount := 10

	wg.Add(routineCount)

	for i := 0; i < routineCount; i++ {
		go executeRequests()
	}

	wg.Wait()
}
