package parse

import (
	"centauri/internal/app/record"
)

type CreateTableData struct {
	tableName string
	schema    *record.Schema
}

func NewCreateTableData(tableName string, schema *record.Schema) *CreateTableData {
	return &CreateTableData{
		tableName: tableName,
		schema:    schema,
	}
}

func (cd *CreateTableData) TableName() string {
	return cd.tableName
}

func (cd *CreateTableData) NewSchema() *record.Schema {
	return cd.schema
}
