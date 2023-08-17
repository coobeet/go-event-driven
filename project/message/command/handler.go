package command

import (
	"context"
	"tickets/entities"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

type Handler struct {
	eventBus *cqrs.EventBus

	receiptsServiceClient ReceiptsService
	paymentsServiceClient PaymentsService
}

func NewHandler(eventBus *cqrs.EventBus, receiptsServiceClient ReceiptsService, paymentsServiceClient PaymentsService) Handler {
	if eventBus == nil {
		panic("eventBus is required")
	}
	if receiptsServiceClient == nil {
		panic("receiptsServiceClient is required")
	}
	if paymentsServiceClient == nil {
		panic("paymentsServiceClient is required")
	}

	handler := Handler{
		eventBus:              eventBus,
		receiptsServiceClient: receiptsServiceClient,
		paymentsServiceClient: paymentsServiceClient,
	}

	return handler
}

type ReceiptsService interface {
	VoidReceipt(ctx context.Context, request entities.VoidReceipt) error
}

type PaymentsService interface {
	RefundPayment(ctx context.Context, request entities.PaymentRefund) error
}
