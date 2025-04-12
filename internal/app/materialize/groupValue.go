package materialize

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/types"
)

// Represents a combination of field values that identify a group
// It holds the values of the grouping field for the current record of a scan
type GroupValue struct {
	vals map[string]*types.Constant
}

func NewGroupValue(s interfaces.Scan, fields []string) *GroupValue {
	vals := make(map[string]*types.Constant)

	for _, fieldName := range fields {
		vals[fieldName] = s.GetVal(fieldName)
	}

	return &GroupValue{
		vals: vals,
	}
}

func (gv *GroupValue) GetVal(fieldName string) *types.Constant {
	return gv.vals[fieldName]
}

// Determines if two GroupValue objects are equal.
func (gv *GroupValue) Equals(other *GroupValue) bool {
	for fieldName, v1 := range gv.vals {
		v2, exists := other.vals[fieldName]
		if !exists || !v1.Equals(v2) {
			return false
		}
	}
	return true
}

// Returns the hash code for this GroupValue.
// The hash code is the sum of the hash codes of its field values.
func (gv *GroupValue) HashCode() int {
	hashVal := 0
	for _, c := range gv.vals {
		hashVal += int(c.HashCode())
	}

	return hashVal
}
