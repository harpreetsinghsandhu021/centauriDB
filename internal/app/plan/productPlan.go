package plan

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/query"
	"centauri/internal/app/record/schema"
)

// Implements a Cartesian product operation in the query execution plan
// It combines every record from the first plan with every record from the second plan
type ProductPlan struct {
	p1     interfaces.Plan
	p2     interfaces.Plan
	schema *schema.Schema
}

func NewProductPlan(p1 interfaces.Plan, p2 interfaces.Plan) *ProductPlan {
	schema := schema.NewSchema()
	schema.AddAll(p1.Schema())
	schema.AddAll(p2.Schema())

	return &ProductPlan{
		p1:     p1,
		p2:     p2,
		schema: schema,
	}
}

// Initializes the scan operation for this plan
// Creates a new ProductScan that combines records from both input plans
func (pp *ProductPlan) Open() interfaces.Scan {
	s1 := pp.p1.Open()
	s2 := pp.p2.Open()
	return query.NewProductScan(s1, s2)
}

// Returns the estimated number of disk blocks that need to be read
// for a product operation, we need:
//   - All Blocks from p1
//   - For each record in p1, all blocks in p2
//
// Total B1 + (R1 * B2), where B=blocks, R=records
func (pp *ProductPlan) BlocksAccessed() int {
	return pp.p1.BlocksAccessed() + (pp.p1.RecordsOutput() * pp.p2.BlocksAccessed())
}

// Returns the estimated number of records in the output
// For a cartesian product, this is the product of the number of records
// from both input plans
func (pp *ProductPlan) RecordsOutput() int {
	return pp.p1.RecordsOutput() * pp.p2.RecordsOutput()
}

// Returns the distinct values for a given field
// If the field is from p1, return p1's distinct values
// If the field is from p2, return p2's distinct values
func (pp *ProductPlan) DistinctValues(fieldName string) int {
	if pp.p1.Schema().HasField(fieldName) {
		return pp.p1.DistinctValues(fieldName)
	} else {
		return pp.p2.DistinctValues(fieldName)
	}
}

// Returns the combined schema containing fields from both input plans
func (pp *ProductPlan) Schema() *schema.Schema {
	return pp.schema
}
