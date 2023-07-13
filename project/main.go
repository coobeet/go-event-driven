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
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/labstack/echo/v4"
	"github.com/lithammer/shortuuid/v3"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type TicketsConfirmationRequest struct {
	Tickets []string `json:"tickets"`
}

type TicketStatus struct {
	TicketID      string `json:"ticket_id"`
	Status        string `json:"status"`
	CustomerEmail string `json:"customer_email"`
	Price         Money  `json:"price"`
}

type TicketsStatusRequest struct {
	Tickets []TicketStatus `json:"tickets"`
}

type EventHeader struct {
	ID          string    `json:"id"`
	PublishedAt time.Time `json:"published_at"`
}

type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type IssueReceiptRequest struct {
	TicketID string
	Price    Money
}

type TicketBookingConfirmed struct {
	Header        EventHeader `json:"header"`
	TicketID      string      `json:"ticket_id"`
	CustomerEmail string      `json:"customer_email"`
	Price         Money       `json:"price"`
}

type TicketBookingCanceled struct {
	Header        EventHeader `json:"header"`
	TicketID      string      `json:"ticket_id"`
	CustomerEmail string      `json:"customer_email"`
	Price         Money       `json:"price"`
}

func main() {
	log.Init(logrus.InfoLevel)
	watermillLogger := log.NewWatermill(logrus.NewEntry(logrus.StandardLogger()))

	clients, err := clients.NewClients(
		os.Getenv("GATEWAY_ADDR"),
		func(ctx context.Context, req *http.Request) error {
			req.Header.Set("Correlation-ID", log.CorrelationIDFromContext(ctx))
			return nil
		},
	)
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

	issueReceiptSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "receipts",
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	appendToTrackerSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "spreadsheets",
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	cancelTicketSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "refund-spreadsheets",
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	e := commonHTTP.NewEcho()

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	e.POST("/tickets-status", func(c echo.Context) error {
		var request TicketsStatusRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			switch ticket.Status {
			case "confirmed":
				event := TicketBookingConfirmed{
					Header:        NewHeader(),
					TicketID:      ticket.TicketID,
					CustomerEmail: ticket.CustomerEmail,
					Price: Money{
						Amount:   ticket.Price.Amount,
						Currency: ticket.Price.Currency,
					},
				}

				payload, err := json.Marshal(event)
				if err != nil {
					return err
				}

				msg := message.NewMessage(watermill.NewUUID(), payload)
				msg.Metadata.Set("correlation_id", c.Request().Header.Get("Correlation-ID"))
				msg.Metadata.Set("type", "TicketBookingConfirmed")

				if err := pub.Publish("TicketBookingConfirmed", msg); err != nil {
					return err
				}

			case "canceled":
				event := TicketBookingCanceled{
					Header:        NewHeader(),
					TicketID:      ticket.TicketID,
					CustomerEmail: ticket.CustomerEmail,
					Price: Money{
						Amount:   ticket.Price.Amount,
						Currency: ticket.Price.Currency,
					},
				}

				payload, err := json.Marshal(event)
				if err != nil {
					return err
				}

				msg := message.NewMessage(watermill.NewUUID(), payload)
				msg.Metadata.Set("correlation_id", c.Request().Header.Get("Correlation-ID"))
				msg.Metadata.Set("type", "TicketBookingCanceled")

				if err := pub.Publish("TicketBookingCanceled", msg); err != nil {
					return err
				}

			default:
				return fmt.Errorf("unknown ticket status: %s", ticket.Status)
			}
		}

		return c.NoContent(http.StatusOK)
	})

	router, err := message.NewRouter(message.RouterConfig{}, watermillLogger)
	if err != nil {
		panic(err)
	}

	router.AddMiddleware(func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			ctx := msg.Context()

			reqCollerationID := msg.Metadata.Get("correlation_id")
			if reqCollerationID == "" {
				reqCollerationID = shortuuid.New()
			}

			ctx = log.ContextWithCorrelationID(ctx, reqCollerationID)
			ctx = log.ToContext(ctx, logrus.WithFields(logrus.Fields{"correlation_id": reqCollerationID}))

			msg.SetContext(ctx)

			return h(msg)
		}
	})
	router.AddMiddleware(func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			logger := log.FromContext(msg.Context())
			logger = logger.WithField("message_uuid", msg.UUID)

			logger.Info("Handling a message")

			msgs, err := h(msg)
			if err != nil {
				logger.WithError(err).Error("Message handling error")
			}

			return msgs, err
		}
	})
	router.AddMiddleware(middleware.Retry{
		MaxRetries:      10,
		InitialInterval: time.Millisecond * 100,
		MaxInterval:     time.Second,
		Multiplier:      2,
		Logger:          watermillLogger,
	}.Middleware)
	router.AddMiddleware(func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			logger := log.FromContext(msg.Context())

			if msg.UUID == "2beaf5bc-d5e4-4653-b075-2b36bbf28949" {
				return nil, nil
			}

			if msg.Metadata.Get("type") == "" {
				logger.Error("Invalid message type", nil)
				return nil, nil
			}

			return h(msg)
		}
	})

	router.AddNoPublisherHandler(
		"issue_receipt",
		"TicketBookingConfirmed",
		issueReceiptSub,
		func(msg *message.Message) error {
			var event TicketBookingConfirmed
			if err := json.Unmarshal(msg.Payload, &event); err != nil {
				return err
			}

			if event.Price.Currency == "" {
				event.Price.Currency = "USD"
			}

			return receiptsClient.IssueReceipt(msg.Context(), IssueReceiptRequest{
				TicketID: event.TicketID,
				Price: Money{
					Amount:   event.Price.Amount,
					Currency: event.Price.Currency,
				},
			})
		},
	)

	router.AddNoPublisherHandler(
		"print_ticket",
		"TicketBookingConfirmed",
		appendToTrackerSub,
		func(msg *message.Message) error {
			var event TicketBookingConfirmed
			if err := json.Unmarshal(msg.Payload, &event); err != nil {
				return err
			}

			if event.Price.Currency == "" {
				event.Price.Currency = "USD"
			}

			return spreadsheetsClient.AppendRow(msg.Context(), "tickets-to-print", []string{
				event.TicketID,
				event.CustomerEmail,
				event.Price.Amount,
				event.Price.Currency,
			})
		},
	)

	router.AddNoPublisherHandler(
		"cancel_ticket",
		"TicketBookingCanceled",
		cancelTicketSub,
		func(msg *message.Message) error {
			var event TicketBookingCanceled
			if err := json.Unmarshal(msg.Payload, &event); err != nil {
				return err
			}

			if event.Price.Currency == "" {
				event.Price.Currency = "USD"
			}

			return spreadsheetsClient.AppendRow(msg.Context(), "tickets-to-refund", []string{
				event.TicketID,
				event.CustomerEmail,
				event.Price.Amount,
				event.Price.Currency,
			})
		},
	)

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

func NewHeader() EventHeader {
	return EventHeader{
		ID:          watermill.NewUUID(),
		PublishedAt: time.Now(),
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
