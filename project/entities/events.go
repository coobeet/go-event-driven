package entities

import (
	"time"

	"github.com/google/uuid"
)

type Event interface {
	IsInternal() bool
}

type EventHeader struct {
	ID             string    `json:"id"`
	PublishedAt    time.Time `json:"published_at"`
	IdempotencyKey string    `json:"idempotency_key"`
}

func NewEventHeader() EventHeader {
	return EventHeader{
		ID:             uuid.NewString(),
		PublishedAt:    time.Now().UTC(),
		IdempotencyKey: uuid.NewString(),
	}
}

func NewEventHeaderWithIdempotencyKey(idempotencyKey string) EventHeader {
	return EventHeader{
		ID:             uuid.NewString(),
		PublishedAt:    time.Now().UTC(),
		IdempotencyKey: idempotencyKey,
	}
}

type TicketBookingConfirmed_v1 struct {
	Header EventHeader `json:"header"`

	TicketID      string `json:"ticket_id"`
	CustomerEmail string `json:"customer_email"`
	Price         Money  `json:"price"`

	BookingID string `json:"booking_id"`
}

func (t TicketBookingConfirmed_v1) IsInternal() bool {
	return false
}

type TicketBookingCanceled_v1 struct {
	Header EventHeader `json:"header"`

	TicketID      string `json:"ticket_id"`
	CustomerEmail string `json:"customer_email"`
	Price         Money  `json:"price"`
}

func (t TicketBookingCanceled_v1) IsInternal() bool {
	return false
}

type TicketRefunded_v1 struct {
	Header EventHeader `json:"header"`

	TicketID string `json:"ticket_id"`
}

func (t TicketRefunded_v1) IsInternal() bool {
	return false
}

type TicketPrinted_v1 struct {
	Header EventHeader `json:"header"`

	TicketID string `json:"ticket_id"`
	FileName string `json:"file_name"`
}

func (t TicketPrinted_v1) IsInternal() bool {
	return false
}

type TicketReceiptIssued_v1 struct {
	Header EventHeader `json:"header"`

	TicketID      string `json:"ticket_id"`
	ReceiptNumber string `json:"receipt_number"`

	IssuedAt time.Time `json:"issued_at"`
}

func (t TicketReceiptIssued_v1) IsInternal() bool {
	return false
}

type BookingMade_v1 struct {
	Header EventHeader `json:"header"`

	NumberOfTickets int `json:"number_of_tickets"`

	BookingID uuid.UUID `json:"booking_id"`

	CustomerEmail string    `json:"customer_email"`
	ShowId        uuid.UUID `json:"show_id"`
}

func (b BookingMade_v1) IsInternal() bool {
	return false
}

type InternalOpsReadModelUpdated struct {
	Header EventHeader `json:"header"`

	BookingID uuid.UUID `json:"booking_id"`
}

func (i InternalOpsReadModelUpdated) IsInternal() bool {
	return true
}
