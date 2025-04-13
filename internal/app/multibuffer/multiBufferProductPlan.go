package multibuffer

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/materialize"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
)

// Implements the Plan interface for multibuffer version of the product operator.
type MultibufferProductPlan struct {
	interfaces.Plan
	tx     *tx.Transaction
	lhs    interfaces.Plan
	rhs    interfaces.Plan
	schema schema.Schema
}

func NewMultiBufferProductPlan(tx *tx.Transaction, lhs, rhs interfaces.Plan) interfaces.Plan {
	schema := schema.NewSchema()
	// Materialize the lhs for better performance
	materializedLHS := materialize.NewMaterializePlan(tx, lhs)

	p := &MultibufferProductPlan{
		tx:     tx,
		lhs:    materializedLHS,
		rhs:    rhs,
		schema: *schema,
	}

	// Add all fields from both schemas to our combined schema
	schema.AddAll(lhs.Schema())
	schema.AddAll(rhs.Schema())

	return p
}

// Implements the plan.Plan Open method
// A Scan for this query is created and returned, as follows:
// First, it materializes the LHs and RHS queries.
// It then determines the optimal chunk size, based on the size of the materialized
// RHS file and the number of available buffers. It creates a chunk plan for each chunk, and
// creates a multiscan for the plans.
func (p *MultibufferProductPlan) Open() interfaces.Scan {
	// Open the left scan
	leftScan := p.lhs.Open()
	// Copy records from the right plan into a temporary table
	tempTable := p.copyRecordsFrom(p.rhs)
	return NewMultiBufferProductScan(p.tx, leftScan, tempTable.TableName(), tempTable.GetLayout())
}

// Returns an estimate of the number of block accesses required to execute the query.
// The formula is B(product(p1,p2)) = B(p2) + B(p1)*C(p2)
// where C(p2) is the number of chunks of p2.
func (p *MultibufferProductPlan) BlocksAccessed() int {
	// Calculate the number of chunks based on available buffers
	avail := p.tx.AvailableBuffers()
	size := materialize.NewMaterializePlan(p.tx, p.rhs).BlocksAccessed()
	numChunks := size / avail

	// If there's a remainder, we need an addiotional chunk
	if size%avail > 0 {
		numChunks++
	}

	// Return the total blocks accesses using the formula
	return p.rhs.BlocksAccessed() + (p.lhs.BlocksAccessed() * numChunks)
}

// Estimates the number of output records in the product.
// The formula is: R(product(p1,p2)) = R(p1)*R(p2)
func (p *MultibufferProductPlan) RecordsOutput() int {
	return p.lhs.RecordsOutput() * p.rhs.RecordsOutput()
}

// Estimates the distinct number of field values in the product.
// Since the product does not increase or decrease field values,
// the estimate is the same as in the appropriate underlying query.
func (p *MultibufferProductPlan) DistinctValues(fieldName string) int {
	if p.lhs.Schema().HasField(fieldName) {
		return p.lhs.DistinctValues(fieldName)
	}

	return p.rhs.DistinctValues(fieldName)
}

// Returns the schema of the product, which is the union of the schemas of the underlying queries.
func (p *MultibufferProductPlan) Schema() *schema.Schema {
	return &p.schema
}

// Copies all records from the specified plan into a newly created temp table.
func (p *MultibufferProductPlan) copyRecordsFrom(sourcePlan interfaces.Plan) *materialize.TempTable {
	// Open the source scan and get its schema
	src := sourcePlan.Open()
	sch := sourcePlan.Schema()
	// Create a temp table with the same schema
	tempTable := materialize.NewTempTable(p.tx, sch)

	// Open the destination as an UpdateScan to insert records
	dest := tempTable.Open()

	// Copy all records from source to destination
	for src.Next() {
		dest.Insert()
		// Copy each fields value
		for _, fieldName := range sch.Fields() {
			dest.SetVal(fieldName, src.GetVal(fieldName))
		}
	}

	src.Close()
	dest.Close()

	return tempTable
}
