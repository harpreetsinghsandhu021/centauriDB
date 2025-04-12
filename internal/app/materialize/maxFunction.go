package materialize

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/types"
)

// Implements the max aggregation function.
// It keeps track of the maximum value seen for a specified field
type MaxFunction struct {
	fieldName string
	val       *types.Constant
}

func NewMaxFn(fieldName string) *MaxFunction {
	return &MaxFunction{
		fieldName: fieldName,
	}
}

// Starts a new maximum to be the field value in the current record.
// This is called for the first record in the group.
func (m *MaxFunction) ProcessFirst(s interfaces.Scan) {
	m.val = s.GetVal(m.fieldName)
}

func (m *MaxFunction) ProcessNext(s interfaces.Scan) {
	newVal := s.GetVal(m.fieldName)
	// CompareTo returns > 0 if newVal > m.val
	if newVal.CompareTo(m.val) > 0 {
		m.val = newVal
	}
}

func (m *MaxFunction) FieldName() string {
	return "maxof" + m.fieldName
}

func (m *MaxFunction) Value() *types.Constant {
	return m.val
}
