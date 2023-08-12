package main

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	watermillSQL "github.com/ThreeDotsLabs/watermill-sql/v2/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

func RunForwarder(
	db *sqlx.DB,
	rdb *redis.Client,
	outboxTopic string,
	logger watermill.LoggerAdapter,
) error {
	// your code goes here
	postgresSub, err := watermillSQL.NewSubscriber(
		db,
		watermillSQL.SubscriberConfig{
			SchemaAdapter:  watermillSQL.DefaultPostgreSQLSchema{},
			OffsetsAdapter: watermillSQL.DefaultPostgreSQLOffsetsAdapter{},
		},
		logger,
	)
	if err != nil {
		return err
	}

	err = postgresSub.SubscribeInitialize(outboxTopic)
	if err != nil {
		return err
	}

	redisPub, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		return err
	}

	fwd, err := forwarder.NewForwarder(
		postgresSub,
		redisPub,
		logger,
		forwarder.Config{
			ForwarderTopic: outboxTopic,
			Middlewares: []message.HandlerMiddleware{
				func(h message.HandlerFunc) message.HandlerFunc {
					return func(msg *message.Message) ([]*message.Message, error) {
						fmt.Println("Forwarding message", msg.UUID, string(msg.Payload), msg.Metadata)

						return h(msg)
					}
				},
			},
		},
	)
	if err != nil {
		return err
	}

	go func() {
		err := fwd.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	<-fwd.Running()

	return nil
}
