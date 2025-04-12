package materialize

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/types"
)

// Represents a scan for the groupby operator
// It processes records from an underlying scan, grouping them based on specified fields
// and computing aggregations for each group.
type GroupByScan struct {
	interfaces.Scan
	s           interfaces.Scan
	groupFields []string
	aggFns      []AggregateFunction
	groupVal    *GroupValue
	moreGroups  bool
}

func NewGroupByScan(s interfaces.Scan, groupFields []string, aggFns []AggregateFunction) *GroupByScan {
	gbs := &GroupByScan{
		s:           s,
		groupFields: groupFields,
		aggFns:      aggFns,
	}

	gbs.BeforeFirst()
	return gbs
}

// Positions the scan before the first group. Internally, the underlying scan is always positioned
// at the first record of a group, which means that this method moves to the first underlying record.
func (gbs *GroupByScan) BeforeFirst() {
	gbs.s.BeforeFirst()
	gbs.moreGroups = gbs.s.Next()
}

// Moves to the next group.
// The key of the group is determined by the group values at the current record.
// The method repeatedly reads underlying records until it encounters a record having a different key.
// The aggregation function are called for each record in the group.
// The values of the grouping fields for the group are saved.
func (gbs *GroupByScan) Next() bool {
	if !gbs.moreGroups {
		return false
	}

	// process the first record in the group
	for _, fn := range gbs.aggFns {
		fn.ProcessFirst(gbs.s)
	}

	// Save the group value
	gbs.groupVal = NewGroupValue(gbs.s, gbs.groupFields)

	// Process remaining records in the group
	for {
		// Try to advance to next record
		gbs.moreGroups = gbs.s.Next()
		if !gbs.moreGroups {
			break
		}

		// Check if next record belongs to a different group
		nextGroupVal := NewGroupValue(gbs.s, gbs.groupFields)
		if !gbs.groupVal.Equals(nextGroupVal) {
			break // found the start of the next group
		}

		// Process this record for the current group
		for _, fn := range gbs.aggFns {
			fn.ProcessNext(gbs.s)
		}
	}
	return true
}

func (gbs *GroupByScan) Close() {
	gbs.s.Close()
}

// Gets the constant value of the specified field.
// If the field is a grouping field, then its value can be obtained from the saved group value.
// Otherwise, the value is obtained from the appropriate aggregation function.
func (gbs *GroupByScan) GetVal(fieldName string) *types.Constant {
	// Check if it's a grouping field
	for _, field := range gbs.groupFields {
		if field == fieldName {
			return gbs.groupVal.GetVal(fieldName)
		}
	}

	// Check if it's an aggregation field
	for _, fn := range gbs.aggFns {
		if fn.FieldName() == fieldName {
			return fn.value()
		}
	}

	panic("field" + fieldName + "not found")
}

func (gbs *GroupByScan) GetInt(fieldName string) int {
	return *gbs.GetVal(fieldName).AsInt()
}

func (gbs *GroupByScan) GetString(fieldName string) string {
	return *gbs.GetVal(fieldName).AsString()
}

// Returns true if the specified field is either a grouping field or created by an aggregation fn.
func (gbs *GroupByScan) HasField(fieldName string) bool {
	for _, field := range gbs.groupFields {
		if field == fieldName {
			return true
		}
	}

	for _, fn := range gbs.aggFns {
		if fn.FieldName() == fieldName {
			return true
		}
	}

	return false
}
