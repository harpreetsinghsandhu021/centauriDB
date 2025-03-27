package query

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/types"
	"errors"
	"go/constant"
)

// Implements the updateScan interface for selections.
// It filters records from an underlying scan based on a predicate.
// The scan provides both read and update operations on the filtered records.
type SelectSpan struct {
	interfaces.UpdateScan
	s    interfaces.Scan // The underlying scan
	pred *Predicate      // The selection predicate
}

func NewSelectSpan(s interfaces.Scan, pred *Predicate) *SelectSpan {
	return &SelectSpan{
		s:    s,
		pred: pred,
	}
}

// Scan Interface implementation methods

// Positions the scn before the first record.
func (ss *SelectSpan) BeforeFirst() {
	ss.s.BeforeFirst()
}

// Advances to the next record satisfying the predicate.
func (ss *SelectSpan) Next() bool {
	for ss.s.Next() {
		if ss.pred.IsSatisfied(ss.s) {
			return true
		}
	}
	return false
}

// Returns an integer value from the current record.
func (ss *SelectSpan) GetInt(fieldName string) (int, error) {
	return ss.s.GetInt(fieldName)
}

func (ss *SelectSpan) GetString(fieldName string) (string, error) {
	return ss.s.GetString(fieldName)
}

func (ss *SelectSpan) GetVal(fieldName string) (types.Constant, error) {
	return ss.s.GetVal(fieldName)
}

func (ss *SelectSpan) HasField(fieldName string) bool {
	return ss.s.HasField(fieldName)
}

func (ss *SelectSpan) Close() {
	ss.s.Close()
}

// UpdateScan Interface implementation methods

// Modifies an integer field in the current record.
// It first attempts to cast the underlying scan to an UpdateScan.
// Throws errors if underlying scan does`nt support updates or field modification fails
func (ss *SelectSpan) SetInt(fieldName string, val int) error {
	updateScan, ok := ss.s.(interfaces.UpdateScan)

	if !ok {
		return errors.New("Not updatable")
	}

	return updateScan.SetInt(fieldName, val)
}

// Modifies an string field in the current record.
// It first attempts to cast the underlying scan to an UpdateScan.
// Throws errors if underlying scan does`nt support updates or field modification fails
func (ss *SelectSpan) SetString(fieldName string, val string) error {
	updateScan, ok := ss.s.(interfaces.UpdateScan)

	if !ok {
		return errors.New("Not updatable")
	}

	return updateScan.SetString(fieldName, val)
}

// Modifies a field in the current record using a constant value.
// This method provides type-independent value modification.
func (ss *SelectSpan) SetVal(fieldName string, val constant.Value) error {
	updateScan, ok := ss.s.(interfaces.UpdateScan)

	if !ok {
		return errors.New("Not updatable")
	}

	return updateScan.SetVal(fieldName, val)
}

// Removes the current record from the underlying scan.
// Throws error if underlying scan does`nt support updates or deletion fails
func (ss *SelectSpan) Delete() error {
	updateScan, ok := ss.s.(interfaces.UpdateScan)

	if !ok {
		return errors.New("Not updatable")
	}

	return updateScan.Delete()
}

// Creates a new record in the underlying scan.
// The new record must satisfy the selection predicate to be visible.
func (ss *SelectSpan) Insert() error {
	updateScan, ok := ss.s.(interfaces.UpdateScan)

	if !ok {
		return errors.New("Not updatable")
	}

	return updateScan.Insert()
}

func (ss *SelectSpan) GetRID() (*types.RID, error) {
	updateScan, ok := ss.s.(interfaces.UpdateScan)
	if !ok {
		return nil, errors.New("Not updatable")
	}

	return updateScan.GetRID()
}

// Positions the scan at the specified record.
// The record must satisfy the selection predicate to be accessible.
func (ss *SelectSpan) MoveToRID(rid *types.RID) error {
	updateScan, ok := ss.s.(interfaces.UpdateScan)
	if !ok {
		return errors.New("Not updatable")
	}

	return updateScan.MoveToRID(rid)
}
