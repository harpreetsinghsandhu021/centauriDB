package query

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/types"
	"errors"
)

var ErrFieldNotFound = errors.New("field not found")

// Implements the scan interface for projections.
// It filters fields from an underlying scan based on a field list,
// only allowing access to specified fields.
type ProjectScan struct {
	s         interfaces.Scan
	fieldList []string
}

func NewProjectScan(s interfaces.Scan, fieldList []string) *ProjectScan {
	return &ProjectScan{
		s:         s,
		fieldList: fieldList,
	}
}

// Positions the scan before the first record
func (ps *ProjectScan) BeforeFirst() {
	ps.s.BeforeFirst()
}

// Advances to the next record.
func (ps *ProjectScan) Next() bool {
	return ps.s.Next()
}

func (ps *ProjectScan) GetInt(fieldName string) int {
	if !ps.s.HasField(fieldName) {
		return 0
	}
	return ps.s.GetInt(fieldName)
}

func (ps *ProjectScan) GetString(fieldName string) string {
	if !ps.s.HasField(fieldName) {
		return ""
	}

	return ps.s.GetString(fieldName)
}

func (ps *ProjectScan) GetVal(fieldName string) *types.Constant {
	if !ps.s.HasField(fieldName) {
		return &types.Constant{}
	}

	return ps.s.GetVal(fieldName)
}

func (ps *ProjectScan) HasField(fieldName string) bool {
	for _, f := range ps.fieldList {
		if f == fieldName {
			return true
		}
	}

	return false
}

func (ps *ProjectScan) Close() {
	ps.s.Close()
}
