package query

import (
	"centauri/internal/app/record"
	"strings"
)

// Represents a Boolean combination of terms.
// It implements a conjuction (AND) of terms used to filter records in a database query.
type Predicate struct {
	terms []Term // Terms that are ANDed together to form the complete predicate. An Empty slice represents a predicate that is always true.
}

// Creates an empty predicate, corresponding to "true".
// An empty predicate contains no terms and will be satisfied by all records.
func NewPredicate() *Predicate {
	return &Predicate{
		terms: make([]Term, 0),
	}
}

// Creates a predicate containing a single term.
// for e.g, a term might be "age > 21".
func NewPredicateWithTerm(t Term) *Predicate {
	return &Predicate{
		terms: []Term{t},
	}
}

// Modifies the predicate to be the conjuction (AND) of itself and the specified predicate.
// This effectively combines two predicates by appending all terms from the specified predicate to this
// predicate's terms.
func (p *Predicate) ConjoinWith(pred *Predicate) {
	p.terms = append(p.terms, pred.terms...)
}

// Returns true if the predicate evaluates to true with respect to the specified scan.
// The predicate is satisfied if all of its terms are satisfied.
// An empty predicate (no terms) is always satisfied.
func (p *Predicate) IsSatisfied(s Scan) bool {
	for _, t := range p.terms {
		if !t.IsSatisfied(s) {
			return false
		}
	}
	return true
}

// Calculates the extent to which selecting on the predicate reduces the number of records output
// by a query. For e.g, If the reduction factor is 2, then the predicate cuts the size of the output in half.
//
// The reduction factor of the entire predicate is the product of the reduction factors of its individual terms, as each
// term further filters the result set.
func (p *Predicate) ReductionFactor(plan Plan) int {
	factor := 1

	for _, t := range p.terms {
		factor *= t.ReductionFactor(plan)
	}

	return factor
}

// Returns a new predicate contanining only the terms that can be evaluated using the specified schema.
// A term can be evaluated if all fields it references are in the schema.
func (p *Predicate) SelectSubPred(schema *record.Schema) *Predicate {
	result := NewPredicate()

	for _, t := range p.terms {
		if t.AppliesTo(schema) {
			result.terms = append(result.terms, t)
		}
	}

	if len(result.terms) == 0 {
		return nil
	}

	return result
}

// Returns a new predicate containing terms that can only be evaluated after
// joining two tables (i.e, terms that reference fields from both schemas).
//
// This identifies terms that:
// 1. Cannot be evaluated with only the first schema.
// 2. Cannot be evaluated with only the second schema.
// 3. Can be evaluated with the combined schemas.
func (p *Predicate) JoinSubPred(schema1, schema2 *record.Schema) *Predicate {
	result := NewPredicate()
	newSchema := record.NewSchema()
	newSchema.AddAll(schema1)
	newSchema.AddAll(schema2)

	for _, t := range p.terms {
		if !t.AppliesTo(schema1) && !t.AppliesTo(schema2) && t.AppliesTo(newSchema) {
			result.terms = append(result.terms, t)
		}
	}

	if len(result.terms) == 0 {
		return nil
	}

	return result
}

// Searches for terms of the form "fieldName = constant" and returns the constant
// if such a terms exists for the specified field.
//
// This is useful for query optimization, especially for index selection
// where equality with a constant allows direct record lookup.
func (p *Predicate) EquatesWithConstant(fldName string) *Constant {
	for _, t := range p.terms {
		c := t.EquatesWithConstant(fldName)
		if c != nil {
			return c
		}
	}
	return nil
}

// Searches for terms of the form "fieldName = otherField" and returns the name of the other field if such a
// term exists for the specified field.
//
// This is useful for query optimizations, especially for join operations where equality b/w fields defines
// the join condition.
func (p *Predicate) EquatesWithField(fldName string) string {
	for _, t := range p.terms {
		s := t.EquatesWithField(fldName)
		if s != "" {
			return s
		}
	}

	return ""
}

// Returns a string representation of the predicate.
// Terms are seperated by "AND" in the string representation.
// An empty predicate returns an empty string.
func (p *Predicate) String() string {
	if len(p.terms) == 0 {
		return ""
	}

	var result strings.Builder
	result.WriteString(p.terms[0].String())

	for i := 1; i < len(p.terms); i++ {
		result.WriteString(" AND ")
		result.WriteString(p.terms[i].String())
	}

	return result.String()

}
