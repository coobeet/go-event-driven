package service

import (
	"context"
	"fmt"
	stdHTTP "net/http"

	"tickets/db"
	ticketsHttp "tickets/http"
	"tickets/message"
	"tickets/message/event"

	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	watermillMessage "github.com/ThreeDotsLabs/watermill/message"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

func init() {
	log.Init(logrus.InfoLevel)
}

type Service struct {
	db              *sqlx.DB
	watermillRouter *watermillMessage.Router
	echoRouter      *echo.Echo
}

func New(
	dbConn *sqlx.DB,
	redisClient *redis.Client,
	spreadsheetsService event.SpreadsheetsAPI,
	receiptsService event.ReceiptsService,
	filesAPI event.FilesAPI,
) Service {
	ticketsRepo := db.NewTicketsRepository(dbConn)

	watermillLogger := log.NewWatermill(log.FromContext(context.Background()))

	var redisPublisher watermillMessage.Publisher
	redisPublisher = message.NewRedisPublisher(redisClient, watermillLogger)
	redisPublisher = log.CorrelationPublisherDecorator{Publisher: redisPublisher}

	eventBus := event.NewBus(redisPublisher)

	eventsHandler := event.NewHandler(
		spreadsheetsService,
		receiptsService,
		filesAPI,
		ticketsRepo,
		eventBus,
	)

	eventProcessorConfig := event.NewProcessorConfig(redisClient, watermillLogger)

	watermillRouter := message.NewWatermillRouter(
		eventProcessorConfig,
		eventsHandler,
		watermillLogger,
	)

	echoRouter := ticketsHttp.NewHttpRouter(
		eventBus,
		spreadsheetsService,
		ticketsRepo,
	)

	return Service{
		dbConn,
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
