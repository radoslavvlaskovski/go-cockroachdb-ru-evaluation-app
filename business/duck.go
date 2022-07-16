package business

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgx"
	"github.com/jackc/pgx/v4"
	"log"
	"math/rand"
)

type Duck struct {
	id     int
	name   string
	color  string
	age    uint
	nestId int
}

var names = [10]string{"Daffy", "Daisy", "Donald", "Howard", "Scrooge", "Alfred", "Edd", "Duck", "Charles", "Howard"}
var colors = [4]string{"green", "brown", "yellow", "orange"}

func CreateDucks(conn *pgx.Conn, duckCount int) {
	ctx := context.Background()

	for i := 0; i < duckCount; i++ {
		namePosition := rand.Intn(10)
		colorPosition := rand.Intn(4)
		age := rand.Intn(10) + 1
		nestId := rand.Intn(9999) + 1

		err := crdbpgx.ExecuteTx(ctx, conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx,
				"INSERT INTO duck (name, color, age, nest_id) VALUES ($1, $2, $3, $4)", names[namePosition], colors[colorPosition], uint(age), nestId); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			log.Fatalf("error on inserting in nestId %v : %v", nestId, err)
		}
	}
}

func GetDucks(conn *pgx.Conn, duckCount int) {
	for i := 0; i < duckCount; i++ {
		duckId := rand.Intn(9999) + 1
		query := fmt.Sprintf("SELECT id, name, color, age, nest_id FROM duck where id = %v", duckId)
		rows, err := conn.Query(context.Background(), query)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var duck Duck
			if err := rows.Scan(&duck.id, &duck.name, &duck.color, &duck.age, &duck.nestId); err != nil {
				log.Fatal(err)
			}
			log.Printf("Duck %v: %v\n", duck.id, duck)
		}
	}
}

func DeleteDucks(conn *pgx.Conn, executionCount int) {
	for i := 0; i < executionCount; i++ {
		duckId := rand.Intn(9999) + 1

		ctx := context.Background()
		err := crdbpgx.ExecuteTx(ctx, conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx,
				"DELETE FROM duck WHERE id = $1", duckId); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			log.Fatal("error: ", err)
		}
	}
}

func PaintDucksBlue(conn *pgx.Conn, duckCount int) {
	for i := 0; i < duckCount; i++ {
		duckId := rand.Intn(9999) + 1

		ctx := context.Background()
		err := crdbpgx.ExecuteTx(ctx, conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx,
				"UPDATE duck SET color = 'blue' WHERE id = $1", duckId); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			log.Fatal("error: ", err)
		}
	}
}

func GetAverageEggsPerDuckName(conn *pgx.Conn, executionCount int) {
	for i := 0; i < executionCount; i++ {
		namePosition := rand.Intn(10)
		name := names[namePosition]

		query := fmt.Sprintf("SELECT name, AVG(egg_count) FROM duck INNER JOIN nest ON duck.nest_id = nest.id WHERE duck.name = '%v' GROUP BY name\n\n", name)
		rows, err := conn.Query(context.Background(), query)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			var avg float32

			if err := rows.Scan(&name, &avg); err != nil {
				log.Fatal(err)
			}
			log.Printf("Duck %v: %v\n", name, avg)
		}
	}

}
