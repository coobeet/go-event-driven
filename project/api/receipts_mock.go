package api

import (
	"context"
	"time"

	"tickets/entities"

	"github.com/ThreeDotsLabs/watermill"
)

type ReceiptsMock struct {
	// todo: implement me
	IssuedReceipts []entities.IssueReceiptRequest
}

func (m *ReceiptsMock) IssueReceipt(ctx context.Context, request entities.IssueReceiptRequest) (entities.IssueReceiptResponse, error) {
	m.IssuedReceipts = append(m.IssuedReceipts, request)

	return entities.IssueReceiptResponse{
		ReceiptNumber: watermill.NewUUID(),
		IssuedAt:      time.Now(),
	}, nil
}
