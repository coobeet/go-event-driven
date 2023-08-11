package db_test

import (
	"context"
	"os"
	"sync"
	"testing"
	ticketsDb "tickets/db"
	"tickets/entities"

	_ "github.com/lib/pq"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

var db *sqlx.DB
var getDbOnce sync.Once

func getDb(t *testing.T) *sqlx.DB {
	getDbOnce.Do(func() {
		var err error
		db, err = sqlx.Open("postgres", os.Getenv("POSTGRES_URL"))
		if err != nil {
			panic(err)
		}

		t.Cleanup(func() {
			db.Close()
		})
	})
	return db
}

func TestTicketsRepository_Add_idempotency(t *testing.T) {
	ctx := context.Background()

	db := getDb(t)

	err := ticketsDb.InitializeDatabaseSchema(db)
	require.NoError(t, err)

	repo := ticketsDb.NewTicketsRepository(db)

	ticketToAdd := entities.Ticket{
		TicketID: uuid.NewString(),
		Price: entities.Money{
			Amount:   "30.00",
			Currency: "EUR",
		},
		CustomerEmail: "foo@bar.com",
	}

	for i := 0; i < 2; i++ {
		err = repo.Add(ctx, ticketToAdd)
		require.NoError(t, err)

		// probably it would be good to have a method to get ticket by ID
		tickets, err := repo.FindAll(ctx)
		require.NoError(t, err)

		foundTickets := lo.Filter(tickets, func(t entities.Ticket, _ int) bool {
			return t.TicketID == ticketToAdd.TicketID
		})
		// add should be idempotent, so the method should always return 1
		require.Len(t, foundTickets, 1)
	}
}
