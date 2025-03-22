package query

import (
	"centauri/internal/app/record"
)

// Represents a generic expression that can be either a constant value or a field reference.
// It consists of either a value stored as a Constant, or a field name as a string.
// Only one of val or fldName will be non-zero at any time.
type Expression struct {
	val     *Constant
	fldName string
}

func NewExpressionVal(val *Constant) *Expression {
	return &Expression{
		val: val,
	}
}

func NewExpressionFieldName(fieldName string) *Expression {
	return &Expression{
		fldName: fieldName,
	}
}

func (e *Expression) IsFieldName() bool {
	return e.fldName != ""
}

func (e *Expression) AsConstant() *Constant {
	return e.val
}

func (e *Expression) AsFieldName() string {
	return e.fldName
}

// Processes the expression and returns a Constant value.
// If the expression has a predefined value (e.val), it returns that value.
// Otherwise, it retrieves the value associated with the field name (e.fldName)
// from the provided Scan interface.
func (e *Expression) Evaluate(s Scan) *Constant {
	if e.val != nil {
		return e.val
	}

	val, _ := s.GetVal(e.fldName)
	return &val
}

// AppliesTo checks if the expression is applicable to the given schema.
// If the expression contains a literal value (val), it always returns true.
// Otherwise, it checks if the schema contains the field specified by fldName.
// Parameters:
//   - schema: The record schema to check against
//
// Returns:
//   - bool: true if the expression applies to the schema, false otherwise
func (e *Expression) AppliesTo(schema *record.Schema) bool {
	if e.val != nil {
		return true
	}

	return schema.HasField(e.fldName)
}

func (e *Expression) String() string {
	if e.val != nil {
		return e.val.String()
	}

	return e.fldName
}
