package query

import (
	"errors"
)

var ErrFieldNotFound = errors.New("field not found")

// Implements the scan interface for projections.
// It filters fields from an underlying scan based on a field list,
// only allowing access to specified fields.
type ProjectScan struct {
	s         Scan
	fieldList []string
}

func NewProjectScan(s Scan, fieldList []string) *ProjectScan {
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

func (ps *ProjectScan) GetInt(fieldName string) (int, error) {
	if !ps.s.HasField(fieldName) {
		return 0, ErrFieldNotFound
	}
	return ps.s.GetInt(fieldName)
}

func (ps *ProjectScan) GetString(fieldName string) (string, error) {
	if !ps.s.HasField(fieldName) {
		return "", ErrFieldNotFound
	}

	return ps.s.GetString(fieldName)
}

func (ps *ProjectScan) GetVal(fieldName string) (Constant, error) {
	if !ps.s.HasField(fieldName) {
		return Constant{}, ErrFieldNotFound
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
