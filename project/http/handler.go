package http

import (
	"context"
	"tickets/entities"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

type Handler struct {
	eventBus              *cqrs.EventBus
	spreadsheetsAPIClient SpreadsheetsAPI
	ticketsRepo           TicketsRepository
}

type SpreadsheetsAPI interface {
	AppendRow(ctx context.Context, spreadsheetName string, row []string) error
}

type TicketsRepository interface {
	FindAll(ctx context.Context) ([]entities.Ticket, error)
}
