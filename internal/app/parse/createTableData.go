package parse

import (
	"centauri/internal/app/record/schema"
)

type CreateTableData struct {
	tableName string
	schema    *schema.Schema
}

func NewCreateTableData(tableName string, schema *schema.Schema) *CreateTableData {
	return &CreateTableData{
		tableName: tableName,
		schema:    schema,
	}
}

func (cd *CreateTableData) TableName() string {
	return cd.tableName
}

func (cd *CreateTableData) NewSchema() *schema.Schema {
	return cd.schema
}
