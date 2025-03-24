package parse

import (
	"centauri/internal/app/query"
)

type InsertData struct {
	tableName string
	fields    []string
	values    []*query.Constant
}

func NewInsertData(tableName string, fields []string, values []*query.Constant) *InsertData {
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

func (id *InsertData) Values() []*query.Constant {
	return id.values
}
