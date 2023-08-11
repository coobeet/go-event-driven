package http

import (
	"net/http"

	libHttp "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/labstack/echo/v4"
)

func NewHttpRouter(
	eb *cqrs.EventBus,
	spreadsheetsAPIClient SpreadsheetsAPI,
	ticketsRepo TicketsRepository,
	showsRepo ShowsRepository,
	bookingsRepo BookingsRepository,
) *echo.Echo {
	e := libHttp.NewEcho()

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	handler := Handler{
		eventBus:              eb,
		spreadsheetsAPIClient: spreadsheetsAPIClient,
		ticketsRepo:           ticketsRepo,
		showsRepo:             showsRepo,
		bookingsRepository:    bookingsRepo,
	}

	e.POST("/tickets-status", handler.PostTicketsStatus)

	e.GET("/tickets", handler.GetTickets)
	e.POST("/book-tickets", handler.PostBookTickets)

	e.POST("/shows", handler.PostShows)

	return e
}
