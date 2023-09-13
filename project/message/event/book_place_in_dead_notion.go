package event

import (
	"context"
	"fmt"
	"tickets/entities"

	"github.com/ThreeDotsLabs/go-event-driven/common/log"
)

func (h Handler) BookPlaceInDeadNation(ctx context.Context, event *entities.BookingMade_v1) error {
	log.FromContext(ctx).Info("Booking ticket in Dead Nation")

	show, err := h.showsRepository.ShowByID(ctx, event.ShowId)
	if err != nil {
		return fmt.Errorf("failed to get show: %w", err)
	}

	err = h.deadNationAPI.BookInDeadNation(ctx, entities.DeadNationBooking{
		CustomerEmail:     event.CustomerEmail,
		DeadNationEventID: show.DeadNationId,
		NumberOfTickets:   event.NumberOfTickets,
		BookingID:         event.BookingID,
	})
	if err != nil {
		return fmt.Errorf("failed to book in dead nation: %w", err)
	}

	return nil
}
