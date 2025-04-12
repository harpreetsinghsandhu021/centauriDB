package materialize

import "centauri/internal/app/interfaces"

// Implments comparison of database records for sorting operations.
// It compares records based on a specified list of fields in priority order.
// Key characterstics:
// - Compares records from two Scan instances field-by-field
// - Supports multi-field sorting (primary, secondary, etc. keys)
// - Returns ordering according to first non-equal field comparison
// - Implements consistent ordering for stable sorting
type RecordComparator struct {
	fields []string
}

func NewRecordComparator(fields []string) *RecordComparator {
	return &RecordComparator{
		fields: fields,
	}
}

// Compares the current records of two scans according to the field list.
// The comparison follows these rules:
// 1. Fields are evaluated in the order specified during construction
// 2. For each field, the corresponding values are compared
// 3. The first non-zero comparison result determines the overall ordering
// 4. If all fields compare equal, return 0
func (rc *RecordComparator) Compare(s1, s2 interfaces.Scan) int {
	for _, fieldName := range rc.fields {
		val1 := s1.GetVal(fieldName)
		val2 := s2.GetVal(fieldName)

		// Compare the two field values
		result := val1.CompareTo(val2)
		if result != 0 {
			return result
		}
	}
	return 0
}
