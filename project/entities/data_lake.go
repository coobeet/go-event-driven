package entities

import "time"

type DataLakeEvent struct {
	EventID      string    `db:"event_id"`
	PublishedAt  time.Time `db:"published_at"`
	EventName    string    `db:"event_name"`
	EventPayload []byte    `db:"event_payload"`
}
