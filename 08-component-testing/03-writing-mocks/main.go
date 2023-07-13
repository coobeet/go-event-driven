package main

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill"
)

type IssueReceiptRequest struct {
	TicketID string `json:"ticket_id"`
	Price    Money  `json:"price"`
}

type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type IssueReceiptResponse struct {
	ReceiptNumber string    `json:"number"`
	IssuedAt      time.Time `json:"issued_at"`
}

type ReceiptsService interface {
	IssueReceipt(ctx context.Context, request IssueReceiptRequest) (IssueReceiptResponse, error)
}

type ReceiptsServiceMock struct {
	// todo: implement me
	IssuedReceipts []IssueReceiptRequest
}

func (m *ReceiptsServiceMock) IssueReceipt(ctx context.Context, request IssueReceiptRequest) (IssueReceiptResponse, error) {
	m.IssuedReceipts = append(m.IssuedReceipts, request)

	return IssueReceiptResponse{
		ReceiptNumber: watermill.NewUUID(),
		IssuedAt:      time.Now(),
	}, nil
}
