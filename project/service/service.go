package service

import (
	"context"
	"fmt"
	stdHTTP "net/http"
	"tickets/db"
	ticketsHttp "tickets/http"
	"tickets/message"
	"tickets/message/command"
	"tickets/message/event"
	"tickets/message/outbox"
	"time"

	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	watermillMessage "github.com/ThreeDotsLabs/watermill/message"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

func init() {
	log.Init(logrus.InfoLevel)
}

type Service struct {
	db *sqlx.DB

	dataLake     db.DataLake
	opsReadModel db.OpsBookingReadModel

	watermillRouter *watermillMessage.Router
	echoRouter      *echo.Echo
}

type ReceiptService interface {
	event.ReceiptsService
	command.ReceiptsService
}

func recordMetrics() {
	go func() {
		for {
			veryImportantCounter.Inc()
			time.Sleep(time.Millisecond * 100)
		}
	}()
}

var (
	veryImportantCounter = promauto.NewCounter(prometheus.CounterOpts{
		// metric will be named tickets_very_important_counter_total
		Namespace: "tickets",
		Name:      "very_important_counter_total",
		Help:      "Total number of very important things processed",
	})
)

func New(
	dbConn *sqlx.DB,
	redisClient *redis.Client,
	deadNationAPI event.DeadNationAPI,
	spreadsheetsService event.SpreadsheetsAPI,
	receiptsService ReceiptService,
	filesAPI event.FilesAPI,
	paymentsService command.PaymentsService,
) Service {
	watermillLogger := log.NewWatermill(log.FromContext(context.Background()))

	recordMetrics()

	var redisPublisher watermillMessage.Publisher
	redisPublisher = message.NewRedisPublisher(redisClient, watermillLogger)
	redisPublisher = log.CorrelationPublisherDecorator{Publisher: redisPublisher}

	redisSubscriber := message.NewRedisSubscriber(redisClient, watermillLogger)
	eventBus := event.NewBus(redisPublisher)

	ticketsRepo := db.NewTicketsRepository(dbConn)
	OpsBookingReadModel := db.NewOpsBookingReadModel(dbConn, eventBus)
	showsRepo := db.NewShowsRepository(dbConn)
	bookingsRepository := db.NewBookingsRepository(dbConn)
	dataLake := db.NewDataLake(dbConn)

	eventsHandler := event.NewHandler(
		deadNationAPI,
		spreadsheetsService,
		receiptsService,
		filesAPI,
		ticketsRepo,
		showsRepo,
		eventBus,
	)

	commandsHandler := command.NewHandler(
		eventBus,
		receiptsService,
		paymentsService,
	)

	commandBus := command.NewBus(redisPublisher, command.NewBusConfig(watermillLogger))

	postgresSubscriber := outbox.NewPostgresSubscriber(dbConn.DB, watermillLogger)
	eventProcessorConfig := event.NewProcessorConfig(redisClient, watermillLogger)
	commandProcessorConfig := command.NewProcessorConfig(redisClient, watermillLogger)

	watermillRouter := message.NewWatermillRouter(
		postgresSubscriber,
		redisPublisher,
		redisSubscriber,
		eventProcessorConfig,
		eventsHandler,
		commandProcessorConfig,
		commandsHandler,
		OpsBookingReadModel,
		dataLake,
		watermillLogger,
	)

	echoRouter := ticketsHttp.NewHttpRouter(
		eventBus,
		commandBus,
		spreadsheetsService,
		ticketsRepo,
		OpsBookingReadModel,
		showsRepo,
		bookingsRepository,
	)

	return Service{
		dbConn,
		dataLake,
		OpsBookingReadModel,
		watermillRouter,
		echoRouter,
	}
}

func (s Service) Run(
	ctx context.Context,
) error {
	if err := db.InitializeDatabaseSchema(s.db); err != nil {
		return fmt.Errorf("failed to initialize database schema: %w", err)
	}

	errgrp, ctx := errgroup.WithContext(ctx)

	errgrp.Go(func() error {
		return s.watermillRouter.Run(ctx)
	})

	errgrp.Go(func() error {
		// we don't want to start HTTP server before Watermill router (so service won't be healthy before it's ready)
		<-s.watermillRouter.Running()

		err := s.echoRouter.Start(":8080")

		if err != nil && err != stdHTTP.ErrServerClosed {
			return err
		}

		return nil
	})

	errgrp.Go(func() error {
		<-ctx.Done()
		return s.echoRouter.Shutdown(context.Background())
	})

	return errgrp.Wait()
}
