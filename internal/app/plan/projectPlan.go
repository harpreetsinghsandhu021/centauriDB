package plan

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/query"
	"centauri/internal/app/record/schema"
)

// Implements a projection operation in the query execution plan.
// It selects specific fields from the records produced by the underlying plan
type ProjectPlan struct {
	interfaces.Plan
	p      interfaces.Plan
	schema *schema.Schema
}

func NewProjectPlan(p interfaces.Plan, fieldList []string) *ProjectPlan {
	schema := schema.NewSchema()

	for _, fieldName := range fieldList {
		schema.Add(fieldName, p.Schema())
	}

	return &ProjectPlan{
		p:      p,
		schema: schema,
	}
}

// Initializes the scan operation for this plan
// Creates a new ProjectScan that wraps the underlying plan's
// scan and only returns the fields specified in the schema.
func (pp *ProjectPlan) Open() interfaces.Scan {
	s := pp.p.Open()
	return query.NewProjectScan(s, pp.schema.Fields())
}

// Returns the number of disk blocks that need to be read
// For a project operation, this is the same as the underlying plan
// since we need to scan all blocks to extract the projected fields
func (pp *ProjectPlan) BlocksAccessed() int {
	return pp.p.BlocksAccessed()
}

// Returns the estimated number of records that will be output
// For projection, this is the same as the underlying plan
// since projection only affects columns, not rows
func (pp *ProjectPlan) RecordsOutput() int {
	return pp.p.RecordsOutput()
}

// Returns the number of distinct values for a given field
// in the output. For projection, this is the same as the underlying plan
// since we're just selecting columns, not modifying their values
func (pp *ProjectPlan) DistinctValues(fieldName string) int {
	return pp.p.DistinctValues(fieldName)
}

// Returns the schema of the output records
// This schema only contains the fields specified in the projection
func (pp *ProjectPlan) Schema() *schema.Schema {
	return pp.schema
}
