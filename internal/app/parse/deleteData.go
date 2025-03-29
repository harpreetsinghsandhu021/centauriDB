package parse

import (
	"centauri/internal/app/query"
)

type DeleteData struct {
	tableName string
	pred      *query.Predicate
}

func NewDeleteData(tableName string, pred *query.Predicate) *DeleteData {
	return &DeleteData{
		tableName: tableName,
		pred:      pred,
	}
}

func (dd *DeleteData) TableName() string {
	return dd.tableName
}

func (dd *DeleteData) Pred() *query.Predicate {
	return dd.pred
}
