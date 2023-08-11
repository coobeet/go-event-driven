package event

import (
	"context"

	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/coobeet/go-event-driven/project/entities"
)

func (h Handler) AppendToTracker(ctx context.Context, event entities.TicketBookingConfirmed) error {
	log.FromContext(ctx).Info("Appending ticket to the tracker")

	return h.spreadsheetsService.AppendRow(
		ctx,
		"tickets-to-print",
		[]string{event.TicketID, event.CustomerEmail, event.Price.Amount, event.Price.Currency},
	)
}
