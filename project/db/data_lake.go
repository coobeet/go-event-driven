package db

import (
	"context"
	"errors"
	"fmt"
	"tickets/entities"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type DataLake struct {
	db *sqlx.DB
}

func NewDataLake(db *sqlx.DB) DataLake {
	if db == nil {
		panic("db is nil")
	}

	return DataLake{db: db}
}

func (s DataLake) StoreEvent(
	ctx context.Context,
	eventID string,
	eventHeader entities.EventHeader,
	eventName string,
	payload []byte,
) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO events (event_id, published_at, event_name, event_payload) VALUES ($1, $2, $3, $4)`,
		eventID,
		eventHeader.PublishedAt,
		eventName,
		payload,
	)
	var postgresError *pq.Error
	if errors.As(err, &postgresError) && postgresError.Code.Name() == "unique_violation" {
		// handling re-delivery
		return nil
	}
	if err != nil {
		return fmt.Errorf("could not store %s event in data lake: %w", eventID, err)
	}

	return nil
}
