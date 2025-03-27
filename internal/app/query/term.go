package query

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/record"
	"centauri/internal/app/types"
	"math"
)

// Term represents a logical term in a query expression,
// consisting of left-hand side (lhs) and right-hand side (rhs) expressions.
// It is used to build complex query conditions where two expressions
// are related through some operation or comparison.
type Term struct {
	lhs *Expression
	rhs *Expression
}

func NewTerm(lhs *Expression, rhs *Expression) *Term {
	return &Term{
		lhs: lhs,
		rhs: rhs,
	}
}

// Checks if the term's condition is satisfied by comparing left-hand side
// and right-hand side expressions' evaluated values.
//
// Parameters:
//   - s: A Scan interface that provides access to the current record/row data
//
// Returns:
//   - bool: true if the left and right expressions evaluate to equal values, false otherwise
func (t *Term) IsSatisfied(s interfaces.Scan) bool {
	lhsVal := t.lhs.Evaluate(s)
	rhsVal := t.rhs.Evaluate(s)
	return rhsVal.Equals(lhsVal)
}

// Checks if both the left-hand side (lhs) and right-hand side (rhs) of the term
// are applicable to the given schema. This method is used to validate if the term's operands
// are compatible with the schema structure.
func (t *Term) AppliesTo(schema *record.Schema) bool {
	return t.lhs.AppliesTo(schema) && t.rhs.AppliesTo(schema)
}

// Calculates the estimated reduction factor for a Term when applied to a given Plan.
// This factor represents how much the result set is expected to be reduced when this term`s condition
// is applied during query execution.
//
// Returns:
//   - An Integer representing the estimated reduction factor:
//   - For field-to-field comparisions: maximum distinct value count between the two fields
//   - For field-to-constant comparisions: distinct value count of the field
//   - For equal constants: 1 (maximum reduction)
//   - For non-equal constants: math.MaxInt (no reduction)
func (t *Term) ReductionFactor(p Plan) int {
	var lhsName string
	var rhsName string

	// CASE 1: Both sides of the term are field names
	if t.lhs.IsFieldName() && t.rhs.IsFieldName() {
		lhsName = t.lhs.AsFieldName()
		rhsName = t.rhs.AsFieldName()

		// Return the maximum number of distinct values between between the two fields
		// This is a heuristic that assumes the more distinct values a field has,
		// the less selective (higher reduction factor) the condition will be
		return max(p.distinctValues(lhsName), p.distinctValues(rhsName))
	}

	// CASE 2: Only the left-hand side is a field name
	if t.lhs.IsFieldName() {
		lhsName = t.lhs.AsFieldName()
		// Return the distinct value count for this field
		return p.distinctValues(lhsName)
	}

	// CASE 3: Only the right-hand side is a field name
	if t.rhs.IsFieldName() {
		rhsName = t.rhs.AsFieldName()
		// Return the distinct value count for this field
		return p.distinctValues(rhsName)
	}

	// CASE 4: Both sides are constants and they are equal
	if t.lhs.AsConstant().Equals(t.rhs.AsConstant()) {
		// Equal constants evaluate to a single result(maximum reduction)
		return 1
	}

	// CASE 5: Both sides are constants and they are not equal
	// This condition won't reduce the result set at all
	return math.MaxInt
}

// Checks if the Term represents an equation between the specified field
// and a constant value (e.g., fieldName = constant). It returns the Constant if such an
// equation exists, or nil otherwise.
func (t *Term) EquatesWithConstant(fldName string) *types.Constant {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fldName && !t.rhs.IsFieldName() {
		return t.rhs.AsConstant()
	} else if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fldName && !t.lhs.IsFieldName() {
		return t.lhs.AsConstant()
	} else {
		return nil
	}
}

func (t *Term) EquatesWithField(fldName string) string {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fldName && t.rhs.IsFieldName() {
		return t.rhs.AsFieldName()
	} else if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fldName && !t.lhs.IsFieldName() {
		return t.lhs.AsFieldName()
	} else {
		return ""
	}
}

func (t *Term) String() string {
	return t.lhs.String() + "=" + t.rhs.String()
}
