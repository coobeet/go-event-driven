package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/receipts"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/spreadsheets"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
)

type TicketsConfirmationRequest struct {
	Tickets []string `json:"tickets"`
}

func main() {
	log.Init(logrus.InfoLevel)

	clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
	if err != nil {
		panic(err)
	}

	w := NewWorker(clients)
	go w.Run()

	e := commonHTTP.NewEcho()

	e.POST("/tickets-confirmation", func(c echo.Context) error {
		var request TicketsConfirmationRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			w.Send(Message{Task: TaskIssueReceipt, TicketID: ticket})
			w.Send(Message{Task: TaskAppendToTracker, TicketID: ticket})
		}

		return c.NoContent(http.StatusOK)
	})

	logrus.Info("Server starting...")

	err = e.Start(":8080")
	if err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}

type ReceiptsClient struct {
	clients *clients.Clients
}

func NewReceiptsClient(clients *clients.Clients) ReceiptsClient {
	return ReceiptsClient{
		clients: clients,
	}
}

func (c ReceiptsClient) IssueReceipt(ctx context.Context, ticketID string) error {
	body := receipts.PutReceiptsJSONRequestBody{
		TicketId: ticketID,
	}

	receiptsResp, err := c.clients.Receipts.PutReceiptsWithResponse(ctx, body)
	if err != nil {
		return err
	}
	if receiptsResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", receiptsResp.StatusCode())
	}

	return nil
}

type SpreadsheetsClient struct {
	clients *clients.Clients
}

func NewSpreadsheetsClient(clients *clients.Clients) SpreadsheetsClient {
	return SpreadsheetsClient{
		clients: clients,
	}
}

func (c SpreadsheetsClient) AppendRow(ctx context.Context, spreadsheetName string, row []string) error {
	request := spreadsheets.PostSheetsSheetRowsJSONRequestBody{
		Columns: row,
	}

	sheetsResp, err := c.clients.Spreadsheets.PostSheetsSheetRowsWithResponse(ctx, spreadsheetName, request)
	if err != nil {
		return err
	}
	if sheetsResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", sheetsResp.StatusCode())
	}

	return nil
}

type Task int

const (
	TaskIssueReceipt Task = iota
	TaskAppendToTracker
)

type Message struct {
	Task     Task
	TicketID string
}

type Worker struct {
	queue              chan Message
	receiptsClient     ReceiptsClient
	spreadsheetsClient SpreadsheetsClient
}

func NewWorker(clients *clients.Clients) *Worker {
	receiptsClient := NewReceiptsClient(clients)
	spreadsheetsClient := NewSpreadsheetsClient(clients)
	return &Worker{
		queue:              make(chan Message, 100),
		receiptsClient:     receiptsClient,
		spreadsheetsClient: spreadsheetsClient,
	}
}

func (w *Worker) Send(msg ...Message) {
	for _, m := range msg {
		w.queue <- m
	}
}

func (w *Worker) Run() {
	ctx := context.Background()
	for msg := range w.queue {
		switch msg.Task {
		case TaskIssueReceipt:
			// issue the receipt
			if err := w.receiptsClient.IssueReceipt(ctx, msg.TicketID); err != nil {
				logrus.WithError(err).Error("failed to add receipt for ticket")
				w.Send(msg)
			}
		case TaskAppendToTracker:
			// append to the tracker spreadsheet
			if err := w.spreadsheetsClient.AppendRow(ctx, "tickets-to-print", []string{msg.TicketID}); err != nil {
				logrus.WithError(err).Error("failed to append to tracker spreadsheet")
				w.Send(msg)
			}
		}
	}
}
