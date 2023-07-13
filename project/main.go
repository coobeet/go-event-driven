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
	"github.com/lithammer/shortuuid/v3"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	issueReceiptTopic           = "issue-receipt"
	appendToTrackerTopic        = "append-to-tracker"
	ticketBookingConfirmedTopic = "TicketBookingConfirmed"
	ticketBookingCanceledTopic  = "TicketBookingCanceled"
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

type EventHeader struct {
	ID          string    `json:"id"`
	PublishedAt time.Time `json:"published_at"`
}

type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
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

	refundSpreadsheetSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
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
				payload, err := json.Marshal(TicketBookingConfirmed{
					Header:        EventHeader{ID: watermill.NewUUID(), PublishedAt: time.Now()},
					TicketID:      ticket.TicketID,
					CustomerEmail: ticket.CustomerEmail,
					Price: Money{
						Amount:   ticket.Price.Amount,
						Currency: ticket.Price.Currency,
					},
				})
				if err != nil {
					return err
				}
				msg := message.NewMessage(watermill.NewUUID(), payload)
				msg.Metadata.Set("correlation_id", c.Request().Header.Get("Correlation-ID"))
				if err := pub.Publish(ticketBookingConfirmedTopic, msg); err != nil {
					return err
				}
			case "canceled":
				payload, err := json.Marshal(TicketBookingCanceled{
					Header:        EventHeader{ID: watermill.NewUUID(), PublishedAt: time.Now()},
					TicketID:      ticket.TicketID,
					CustomerEmail: ticket.CustomerEmail,
					Price: Money{
						Amount:   ticket.Price.Amount,
						Currency: ticket.Price.Currency,
					},
				})
				if err != nil {
					return err
				}
				msg := message.NewMessage(watermill.NewUUID(), payload)
				msg.Metadata.Set("correlation_id", c.Request().Header.Get("Correlation-ID"))
				if err := pub.Publish(ticketBookingCanceledTopic, msg); err != nil {
					return err
				}
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

	router.AddNoPublisherHandler(
		issueReceiptTopic,
		ticketBookingConfirmedTopic,
		receiptSub,
		func(msg *message.Message) error {
			var payload TicketBookingConfirmed
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				return err
			}
			return receiptsClient.IssueReceipt(msg.Context(), IssueReceiptRequest{
				TicketID: payload.TicketID,
				Price: PriceMoney{
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
			return spreadsheetsClient.AppendRow(msg.Context(), "tickets-to-print", []string{
				payload.TicketID,
				payload.CustomerEmail,
				payload.Price.Amount,
				payload.Price.Currency,
			})
		},
	)

	router.AddNoPublisherHandler(
		"refund-spreadsheets",
		ticketBookingCanceledTopic,
		refundSpreadsheetSub,
		func(msg *message.Message) error {
			var payload TicketBookingCanceled
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				return err
			}
			return spreadsheetsClient.AppendRow(msg.Context(), "tickets-to-refund", []string{
				payload.TicketID,
				payload.CustomerEmail,
				payload.Price.Amount,
				payload.Price.Currency,
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

type ReceiptsClient struct {
	clients *clients.Clients
}

func NewReceiptsClient(clients *clients.Clients) ReceiptsClient {
	return ReceiptsClient{
		clients: clients,
	}
}

type PriceMoney struct {
	Amount   string
	Currency string
}

type IssueReceiptRequest struct {
	TicketID string
	Price    PriceMoney
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
