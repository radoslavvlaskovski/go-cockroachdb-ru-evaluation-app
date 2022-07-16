package business

import (
	"context"
	"log"
	"math/rand"

	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgx"
	"github.com/jackc/pgx/v4"
)

func CreateNests(conn *pgx.Conn, nestCount int) {
	ctx := context.Background()
	for i := 0; i < nestCount; i++ {
		eggCount := rand.Intn(5)
		err := crdbpgx.ExecuteTx(ctx, conn, pgx.TxOptions{},
			func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx,
					"INSERT INTO nest (egg_count) VALUES ($1)", eggCount)
				if err != nil {
					return err
				}
				return nil
			},
		)
		if err != nil {
			log.Fatal("error: ", err)
		}
	}
}
