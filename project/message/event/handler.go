package event

import (
	"context"

	"tickets/entities"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

type Handler struct {
	spreadsheetsService SpreadsheetsAPI
	receiptsService     ReceiptsService
	filesAPI            FilesAPI
	ticketsRepository   TicketsRepository
	eventBus            *cqrs.EventBus
}

func NewHandler(
	spreadsheetsService SpreadsheetsAPI,
	receiptsService ReceiptsService,
	filesAPI FilesAPI,
	ticketsRepository TicketsRepository,
	eventBus *cqrs.EventBus,
) Handler {
	if spreadsheetsService == nil {
		panic("missing spreadsheetsService")
	}
	if receiptsService == nil {
		panic("missing receiptsService")
	}
	if ticketsRepository == nil {
		panic("missing ticketsRepository")
	}

	return Handler{
		spreadsheetsService: spreadsheetsService,
		receiptsService:     receiptsService,
		filesAPI:            filesAPI,
		ticketsRepository:   ticketsRepository,
		eventBus:            eventBus,
	}
}

type SpreadsheetsAPI interface {
	AppendRow(ctx context.Context, sheetName string, row []string) error
}

type ReceiptsService interface {
	IssueReceipt(ctx context.Context, request entities.IssueReceiptRequest) (entities.IssueReceiptResponse, error)
}

type FilesAPI interface {
	UploadFile(ctx context.Context, fileID string, fileContent string) error
}

type TicketsRepository interface {
	Add(ctx context.Context, ticket entities.Ticket) error
	Remove(ctx context.Context, ticketID string) error
}
