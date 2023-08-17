package api

import (
	"context"
	"sync"
	"tickets/entities"
)

type PaymentsMock struct {
	lock    sync.Mutex
	Refunds []entities.PaymentRefund
}

func (c *PaymentsMock) RefundPayment(ctx context.Context, refundPayment entities.PaymentRefund) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.Refunds = append(c.Refunds, refundPayment)

	return nil
}
