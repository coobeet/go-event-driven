package http

import (
	"fmt"
	"net/http"
	"tickets/entities"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

func (h Handler) PostShows(c echo.Context) error {
	show := entities.Show{}

	if err := c.Bind(&show); err != nil {
		return err
	}

	show.ShowId = uuid.New()

	if err := h.showsRepo.AddShow(c.Request().Context(), show); err != nil {
		return fmt.Errorf("failed to add show: %w", err)
	}

	return c.JSON(http.StatusCreated, show)
}
