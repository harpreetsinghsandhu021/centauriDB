package parse

import (
	"centauri/internal/app/query"
	"strings"
)

// Represents the componnets of a SQL query:
//   - fields to select
//   - tables to query from
//   - predicates for the WHERE clause
type QueryData struct {
	fields []string
	tables []string
	pred   *query.Predicate
}

func NewQueryData(fields []string, tables []string, pred *query.Predicate) *QueryData {
	return &QueryData{
		fields: fields,
		tables: tables,
		pred:   pred,
	}
}

func (qd *QueryData) Fields() []string {
	return qd.fields
}

func (qd *QueryData) Tables() []string {
	return qd.tables
}

func (qd *QueryData) Pred() *query.Predicate {
	return qd.pred
}

// Generates a SQL query string from the QueryData components.
// The method builds a SELECT statement with the specified fields, table and predicate.
func (qd *QueryData) String() string {
	// Use strings.Builder for efficient string concatenation
	var builder strings.Builder

	// Start building with SELECT clause
	builder.WriteString("select ")

	// Add field names with commas
	for i, field := range qd.fields {
		builder.WriteString(field)

		// Add comma and space if not the last field
		if i < len(qd.fields)-1 {
			builder.WriteString(", ")
		}
	}

	builder.WriteString(" from ")

	// Add table names with commas
	for i, table := range qd.tables {
		builder.WriteString(table)
		// Add comma and space if not the last table
		if i < len(qd.tables)-1 {
			builder.WriteString(", ")
		}
	}

	// Add WHERE clause if predicate exists and is not empty
	predString := qd.pred.String()
	if predString != "" {
		builder.WriteString(" where ")
		builder.WriteString(predString)
	}

	return builder.String()
}
