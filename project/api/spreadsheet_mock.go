package api

import "context"

type SpreadsheetsMock struct{}

func (m *SpreadsheetsMock) AppendRow(ctx context.Context, sheetName string, row []string) error {
	return nil
}
