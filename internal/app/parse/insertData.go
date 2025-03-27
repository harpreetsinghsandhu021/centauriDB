package parse

import (
	"centauri/internal/app/types"
)

type InsertData struct {
	tableName string
	fields    []string
	values    []*types.Constant
}

func NewInsertData(tableName string, fields []string, values []*types.Constant) *InsertData {
	return &InsertData{
		tableName: tableName,
		fields:    fields,
		values:    values,
	}
}

func (id *InsertData) TableName() string {
	return id.tableName
}

func (id *InsertData) Fields() []string {
	return id.fields
}

func (id *InsertData) Values() []*types.Constant {
	return id.values
}
