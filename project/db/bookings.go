package db

import (
	"context"
	"errors"
	"fmt"
	"tickets/entities"

	"github.com/jmoiron/sqlx"
)

type BookingsRepository struct {
	db *sqlx.DB
}

func NewBookingsRepository(db *sqlx.DB) BookingsRepository {
	if db == nil {
		panic("nil db")
	}

	return BookingsRepository{db: db}
}

func (b BookingsRepository) AddBooking(ctx context.Context, booking entities.Booking) (err error) {
	tx, err := b.db.Beginx()
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			rollbackErr := tx.Rollback()
			err = errors.Join(err, rollbackErr)
			return
		}
		err = tx.Commit()
	}()

	_, err = tx.NamedExecContext(ctx, `
		INSERT INTO 
		    bookings (booking_id, show_id, number_of_tickets, customer_email) 
		VALUES (:booking_id, :show_id, :number_of_tickets, :customer_email)
		`, booking)
	if err != nil {
		return fmt.Errorf("could not add booking: %w", err)
	}

	return nil
}
