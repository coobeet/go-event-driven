package message

import (
	"fmt"
	"tickets/db"
	"tickets/message/command"
	"tickets/message/event"
	"tickets/message/outbox"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
)

func NewWatermillRouter(
	postgresSubscriber message.Subscriber,
	redisPublisher message.Publisher,
	redisSubscriber message.Subscriber,
	eventProcessorConfig cqrs.EventProcessorConfig,
	eventHandler event.Handler,
	commandProcessorConfig cqrs.CommandProcessorConfig,
	commandsHandler command.Handler,
	opsReadModel db.OpsBookingReadModel,
	watermillLogger watermill.LoggerAdapter,
) *message.Router {
	router, err := message.NewRouter(message.RouterConfig{}, watermillLogger)
	if err != nil {
		panic(err)
	}

	useMiddlewares(router, watermillLogger)

	outbox.AddForwarderHandler(postgresSubscriber, redisPublisher, router, watermillLogger)

	eventProcessor, err := cqrs.NewEventProcessorWithConfig(router, eventProcessorConfig)
	if err != nil {
		panic(err)
	}

	eventProcessor.AddHandlers(
		cqrs.NewEventHandler(
			"BookPlaceInDeadNation",
			eventHandler.BookPlaceInDeadNation,
		),
		cqrs.NewEventHandler(
			"AppendToTracker",
			eventHandler.AppendToTracker,
		),
		cqrs.NewEventHandler(
			"TicketRefundToSheet",
			eventHandler.TicketRefundToSheet,
		),
		cqrs.NewEventHandler(
			"IssueReceipt",
			eventHandler.IssueReceipt,
		),
		cqrs.NewEventHandler(
			"PrintTicketHandler",
			eventHandler.PrintTicket,
		),
		cqrs.NewEventHandler(
			"StoreTickets",
			eventHandler.StoreTickets,
		),
		cqrs.NewEventHandler(
			"RemoveCanceledTicket",
			eventHandler.RemoveCanceledTicket,
		),
		cqrs.NewEventHandler(
			"ops_read_model.OnBookingMade",
			opsReadModel.OnBookingMade,
		),
		cqrs.NewEventHandler(
			"ops_read_model.IssueReceiptHandler",
			opsReadModel.OnTicketReceiptIssued,
		),
		cqrs.NewEventHandler(
			"ops_read_model.OnTicketBookingConfirmed",
			opsReadModel.OnTicketBookingConfirmed,
		),
		cqrs.NewEventHandler(
			"ops_read_model.OnTicketPrinted",
			opsReadModel.OnTicketPrinted,
		),
		cqrs.NewEventHandler(
			"ops_read_model.OnTicketRefunded",
			opsReadModel.OnTicketRefunded,
		),
	)

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(
		router,
		commandProcessorConfig,
	)
	if err != nil {
		panic(err)
	}

	commandProcessor.AddHandlers(
		cqrs.NewCommandHandler(
			"TicketRefund",
			commandsHandler.RefundTicket,
		),
	)

	router.AddNoPublisherHandler(
		"events_splitter",
		"events",
		redisSubscriber,
		func(msg *message.Message) error {
			eventName := eventProcessorConfig.Marshaler.NameFromMessage(msg)
			if eventName == "" {
				return fmt.Errorf("cannot get event name from message")
			}

			return redisPublisher.Publish("events."+eventName, msg)
		},
	)

	return router
}
