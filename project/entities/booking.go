package entities

import "github.com/google/uuid"

type Booking struct {
	BookingID uuid.UUID `json:"booking_id" db:"booking_id"`
	ShowID    uuid.UUID `json:"show_id" db:"show_id"`

	NumberOfTickets int   `json:"number_of_tickets" db:"number_of_tickets"`
	TicketIDs       UUIDs `json:"ticket_ids" db:"ticket_ids"`

	CustomerEmail string `json:"customer_email" db:"customer_email"`
}

type DeadNationBooking struct {
	BookingID         uuid.UUID
	NumberOfTickets   int
	CustomerEmail     string
	DeadNationEventID uuid.UUID
}
