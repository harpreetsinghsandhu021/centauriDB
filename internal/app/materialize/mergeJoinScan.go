package materialize

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/types"
)

type MergeJoinScan struct {
	interfaces.Scan
	s1       interfaces.Scan
	s2       *SortScan
	fldName1 string
	fldName2 string
	joinVal  *types.Constant
}

func NewMergeJoinScan(s1 interfaces.Scan, s2 *SortScan, fldName1, fldName2 string) *MergeJoinScan {
	mjs := &MergeJoinScan{
		s1:       s1,
		s2:       s2,
		fldName1: fldName1,
		fldName2: fldName2,
	}

	mjs.BeforeFirst()
	return mjs
}

func (m *MergeJoinScan) Close() {
	m.s1.Close()
	m.s2.Close()
}

func (m *MergeJoinScan) BeforeFirst() {
	m.s1.BeforeFirst()
	m.s2.BeforeFirst()
}

// Moves to the next record. This is where the action is.
// If the next RHS record has the same join value, the move to it.
// Otherwise, if the next LHS record has the same join value, then reposition the RHS
// scan back to the first record having that join value.
// Otherwise, repeatedly move the scan having the smallest value until a common join value is
// found.
// When one of the scans runs out of records, false otherwise
func (m *MergeJoinScan) Next() bool {
	// Try to move s2 scan to next record with same join value
	hasMore2 := m.s2.Next()
	if hasMore2 && m.joinVal != nil && m.s2.GetVal(m.fldName2).Equals(m.joinVal) {
		return true
	}

	// Try to move s1 scan to next record, keeping s2 at the same group
	hasMore1 := m.s1.Next()
	if hasMore1 && m.joinVal != nil && m.s1.GetVal(m.fldName1).Equals(m.joinVal) {
		m.s2.RestorePosition()
		return true
	}

	// Look for a new join value by moving the scan with smaller value
	for hasMore1 && hasMore2 {
		v1 := m.s1.GetVal(m.fldName1)
		v2 := m.s2.GetVal(m.fldName2)

		// Compare the field values
		cmp := v1.CompareTo(v2)
		if cmp < 0 {
			// s1's value is smaller, advance it
			hasMore1 = m.s1.Next()
		} else if cmp > 0 {
			// s2's value is smaller, advance it
			hasMore2 = m.s2.Next()
		} else {
			// Found a match
			m.s2.SavePosition()
			m.joinVal = m.s2.GetVal(m.fldName2)
			return true
		}
	}

	// One of the scans has no more records
	return false
}

func (m *MergeJoinScan) GetInt(fldname string) int {
	if m.s1.HasField(fldname) {
		return m.s1.GetInt(fldname)
	}
	return m.s2.GetInt(fldname)
}

func (m *MergeJoinScan) GetString(fldname string) string {
	if m.s1.HasField(fldname) {
		return m.s1.GetString(fldname)
	}
	return m.s2.GetString(fldname)
}

func (m *MergeJoinScan) GetVal(fldname string) *types.Constant {
	if m.s1.HasField(fldname) {
		return m.s1.GetVal(fldname)
	}
	return m.s2.GetVal(fldname)
}

func (m *MergeJoinScan) HasField(fldname string) bool {
	return m.s1.HasField(fldname) || m.s2.HasField(fldname)
}
