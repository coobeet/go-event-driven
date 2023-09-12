package http

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

func (h Handler) GetOpsTickets(c echo.Context) error {
	allReservations, err := h.opsBookingReadModel.AllReservations()
	if err != nil {
		return fmt.Errorf("failed to get all reservations: %w", err)
	}

	return c.JSON(http.StatusOK, allReservations)
}

func (h Handler) GetOpsTicket(c echo.Context) error {
	reservation, err := h.opsBookingReadModel.ReservationReadModel(c.Request().Context(), c.Param("id"))
	if err != nil {
		return fmt.Errorf("failed to get reservation: %w", err)
	}

	return c.JSON(http.StatusOK, reservation)
}
