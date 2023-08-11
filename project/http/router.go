package http

import (
	"net/http"

	libHttp "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/labstack/echo/v4"
)

func NewHttpRouter(eb *cqrs.EventBus, spreadsheetsAPIClient SpreadsheetsAPI) *echo.Echo {
	e := libHttp.NewEcho()

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	handler := Handler{
		eventBus:              eb,
		spreadsheetsAPIClient: spreadsheetsAPIClient,
	}

	e.POST("/tickets-status", handler.PostTicketsStatus)

	return e
}
