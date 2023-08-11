package event

import (
	"context"

	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/coobeet/go-event-driven/project/entities"
)

func (h Handler) CancelTicket(ctx context.Context, event entities.TicketBookingCanceled) error {
	log.FromContext(ctx).Info("Adding ticket refund to sheet")

	return h.spreadsheetsService.AppendRow(
		ctx,
		"tickets-to-refund",
		[]string{event.TicketID, event.CustomerEmail, event.Price.Amount, event.Price.Currency},
	)
}
