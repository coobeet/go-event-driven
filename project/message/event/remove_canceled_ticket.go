package event

import (
	"context"
	"tickets/entities"

	"github.com/ThreeDotsLabs/go-event-driven/common/log"
)

func (h Handler) RemoveCanceledTicket(ctx context.Context, event *entities.TicketBookingCanceled_v1) error {
	log.FromContext(ctx).Info("Storing ticket")

	return h.ticketsRepository.Remove(ctx, event.TicketID)
}
