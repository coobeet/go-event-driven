package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/receipts"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/spreadsheets"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	issueReceiptTopic           = "issue-receipt"
	appendToTrackerTopic        = "append-to-tracker"
	ticketBookingConfirmedTopic = "TicketBookingConfirmed"
)

type TicketsConfirmationRequest struct {
	Tickets []string `json:"tickets"`
}

type Ticket struct {
	TicketID      string `json:"ticket_id"`
	Status        string `json:"status"`
	CustomerEmail string `json:"customer_email"`
	Price         struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"price"`
}

type TicketsStatusRequest struct {
	Tickets []Ticket `json:"tickets"`
}

type Header struct {
	ID          string    `json:"id"`
	PublishedAt time.Time `json:"published_at"`
}

type PriceMoney struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type TicketBookingConfirmed struct {
	Header        Header     `json:"header"`
	TicketID      string     `json:"ticket_id"`
	CustomerEmail string     `json:"customer_email"`
	Price         PriceMoney `json:"price"`
}

func main() {
	log.Init(logrus.InfoLevel)
	watermillLogger := log.NewWatermill(logrus.NewEntry(logrus.StandardLogger()))

	clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
	if err != nil {
		panic(err)
	}

	receiptsClient := NewReceiptsClient(clients)
	spreadsheetsClient := NewSpreadsheetsClient(clients)

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	pub, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: rdb,
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	receiptSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "receipts",
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	spreadsheetSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "spreadsheets",
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	router, err := message.NewRouter(message.RouterConfig{}, watermillLogger)
	if err != nil {
		panic(err)
	}

	router.AddNoPublisherHandler(
		issueReceiptTopic,
		ticketBookingConfirmedTopic,
		receiptSub,
		func(msg *message.Message) error {
			var payload TicketBookingConfirmed
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				return err
			}
			return receiptsClient.IssueReceipt(context.Background(), IssueReceiptRequest{
				TicketID: payload.TicketID,
				Price: Money{
					Amount:   payload.Price.Amount,
					Currency: payload.Price.Currency,
				},
			})
		},
	)

	router.AddNoPublisherHandler(
		appendToTrackerTopic,
		ticketBookingConfirmedTopic,
		spreadsheetSub,
		func(msg *message.Message) error {
			var payload TicketBookingConfirmed
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				return err
			}
			return spreadsheetsClient.AppendRow(context.Background(), "tickets-to-print", []string{
				payload.TicketID,
				payload.CustomerEmail,
				payload.Price.Amount,
				payload.Price.Currency,
			})
		},
	)

	e := commonHTTP.NewEcho()

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	e.POST("/tickets-confirmation", func(c echo.Context) error {
		var request TicketsConfirmationRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			msg := message.NewMessage(watermill.NewUUID(), []byte(ticket))
			if err := pub.Publish(issueReceiptTopic, msg); err != nil {
				return err
			}
			if err := pub.Publish(appendToTrackerTopic, msg); err != nil {
				return err
			}
		}

		return c.NoContent(http.StatusOK)
	})

	e.POST("/tickets-status", func(c echo.Context) error {
		var request TicketsStatusRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			payload, err := json.Marshal(TicketBookingConfirmed{
				Header:        Header{ID: watermill.NewUUID(), PublishedAt: time.Now()},
				TicketID:      ticket.TicketID,
				CustomerEmail: ticket.CustomerEmail,
				Price: PriceMoney{
					Amount:   ticket.Price.Amount,
					Currency: ticket.Price.Currency,
				},
			})
			if err != nil {
				return err
			}
			msg := message.NewMessage(watermill.NewUUID(), payload)
			if err := pub.Publish(ticketBookingConfirmedTopic, msg); err != nil {
				return err
			}
		}

		return c.NoContent(http.StatusOK)
	})

	logrus.Info("Server starting...")

	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return router.Run(ctx)
	})

	g.Go(func() error {
		<-router.Running()

		err := e.Start(":8080")
		if err != nil && err != http.ErrServerClosed {
			return err
		}

		return nil
	})

	g.Go(func() error {
		// Shut down the HTTP server
		<-ctx.Done()
		return e.Shutdown(ctx)
	})

	// Will block until all goroutines finish
	err = g.Wait()
	if err != nil {
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

type Money struct {
	Amount   string
	Currency string
}

type IssueReceiptRequest struct {
	TicketID string
	Price    Money
}

func (c ReceiptsClient) IssueReceipt(ctx context.Context, request IssueReceiptRequest) error {
	body := receipts.PutReceiptsJSONRequestBody{
		TicketId: request.TicketID,
		Price: receipts.Money{
			MoneyAmount:   request.Price.Amount,
			MoneyCurrency: request.Price.Currency,
		},
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
