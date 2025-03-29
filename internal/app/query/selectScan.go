package query

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/types"
	"errors"
)

// Implements the updateScan interface for selections.
// It filters records from an underlying scan based on a predicate.
// The scan provides both read and update operations on the filtered records.
type SelectScan struct {
	interfaces.UpdateScan
	s    interfaces.Scan // The underlying scan
	pred *Predicate      // The selection predicate
}

func NewSelectScan(s interfaces.Scan, pred *Predicate) *SelectScan {
	return &SelectScan{
		s:    s,
		pred: pred,
	}
}

// Scan Interface implementation methods

// Positions the scn before the first record.
func (ss *SelectScan) BeforeFirst() {
	ss.s.BeforeFirst()
}

// Advances to the next record satisfying the predicate.
func (ss *SelectScan) Next() bool {
	for ss.s.Next() {
		if ss.pred.IsSatisfied(ss.s) {
			return true
		}
	}
	return false
}

// Returns an integer value from the current record.
func (ss *SelectScan) GetInt(fieldName string) int {
	return ss.s.GetInt(fieldName)
}

func (ss *SelectScan) GetString(fieldName string) string {
	return ss.s.GetString(fieldName)
}

func (ss *SelectScan) GetVal(fieldName string) *types.Constant {
	return ss.s.GetVal(fieldName)
}

func (ss *SelectScan) HasField(fieldName string) bool {
	return ss.s.HasField(fieldName)
}

func (ss *SelectScan) Close() {
	ss.s.Close()
}

// UpdateScan Interface implementation methods

// Modifies an integer field in the current record.
// It first attempts to cast the underlying scan to an UpdateScan.
// Throws errors if underlying scan does`nt support updates or field modification fails
func (ss *SelectScan) SetInt(fieldName string, val int) error {
	updateScan, ok := ss.s.(interfaces.UpdateScan)

	if !ok {
		return errors.New("not updatable")
	}

	return updateScan.SetInt(fieldName, val)
}

// Modifies an string field in the current record.
// It first attempts to cast the underlying scan to an UpdateScan.
// Throws errors if underlying scan does`nt support updates or field modification fails
func (ss *SelectScan) SetString(fieldName string, val string) error {
	updateScan, ok := ss.s.(interfaces.UpdateScan)

	if !ok {
		return errors.New("not updatable")
	}

	return updateScan.SetString(fieldName, val)
}

// Modifies a field in the current record using a constant value.
// This method provides type-independent value modification.
func (ss *SelectScan) SetVal(fieldName string, val *types.Constant) error {
	updateScan, ok := ss.s.(interfaces.UpdateScan)

	if !ok {
		return errors.New("not updatable")
	}

	return updateScan.SetVal(fieldName, val)
}

// Removes the current record from the underlying scan.
// Throws error if underlying scan does`nt support updates or deletion fails
func (ss *SelectScan) Delete() error {
	updateScan, ok := ss.s.(interfaces.UpdateScan)

	if !ok {
		return errors.New("not updatable")
	}

	return updateScan.Delete()
}

// Creates a new record in the underlying scan.
// The new record must satisfy the selection predicate to be visible.
func (ss *SelectScan) Insert() error {
	updateScan, ok := ss.s.(interfaces.UpdateScan)

	if !ok {
		return errors.New("not updatable")
	}

	return updateScan.Insert()
}

func (ss *SelectScan) GetRID() (*types.RID, error) {
	updateScan, ok := ss.s.(interfaces.UpdateScan)
	if !ok {
		return nil, errors.New("not updatable")
	}

	return updateScan.GetRID()
}

// Positions the scan at the specified record.
// The record must satisfy the selection predicate to be accessible.
func (ss *SelectScan) MoveToRID(rid *types.RID) error {
	updateScan, ok := ss.s.(interfaces.UpdateScan)
	if !ok {
		return errors.New("not updatable")
	}

	return updateScan.MoveToRID(rid)
}
