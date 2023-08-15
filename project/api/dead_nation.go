package api

import (
	"context"
	"fmt"
	"net/http"
	"tickets/entities"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/dead_nation"
)

type DeadNationClient struct {
	// we are not mocking this client: it's pointless to use interface here
	clients *clients.Clients
}

func NewDeadNationClient(clients *clients.Clients) *DeadNationClient {
	if clients == nil {
		panic("NewFilesApiClient: clients is nil")
	}

	return &DeadNationClient{clients: clients}
}

func (c DeadNationClient) BookInDeadNation(ctx context.Context, request entities.DeadNationBooking) error {
	resp, err := c.clients.DeadNation.PostTicketBookingWithResponse(
		ctx,
		dead_nation.PostTicketBookingRequest{
			CustomerAddress: request.CustomerEmail,
			EventId:         request.DeadNationEventID,
			NumberOfTickets: request.NumberOfTickets,
			BookingId:       request.BookingID,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to book place in Dead Nation: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code from dead nation: %d", resp.StatusCode())
	}

	return nil
}
