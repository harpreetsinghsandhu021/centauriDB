package query

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/types"
)

// Implements the Scan interface for the product of two scans.
// It combines records from two input scans to produce their Cartesian product.
// For each record in S1, it iterates through all records in s2.
type ProductScan struct {
	s1 interfaces.Scan
	s2 interfaces.Scan
}

func NewProductScan(s1, s2 interfaces.Scan) *ProductScan {
	ps := &ProductScan{
		s1: s1,
		s2: s2,
	}

	s1.Next() // Position at first record of s1
	return ps
}

// Positions the scan before the first record of the product.
// This involves:
//  1. Resetting s1 to its first record
//  2. Resetting s2 to before its first record
func (ps *ProductScan) BeforeFirst() {
	ps.s1.BeforeFirst()
	ps.s1.Next() // Move to first record of s1
	ps.s2.BeforeFirst()
}

// Advances to the next record in the product.
// The scanning pattern is:
// 1. Try to advance s2
// 2. If s2 reaches end, reset s2 and advance s1
func (ps *ProductScan) Next() bool {
	if ps.s2.Next() {
		return true
	}

	// If s2 is exhausted, reset it and try next record in s1
	ps.s2.BeforeFirst()
	return ps.s2.Next() && ps.s1.Next()
}

// Returns an integer value from the current record.
func (ps *ProductScan) GetInt(fieldName string) int {
	if ps.s1.HasField(fieldName) {
		return ps.s1.GetInt(fieldName)
	}

	return ps.s2.GetInt(fieldName)
}

// Returns a string value from the current record.
func (ps *ProductScan) GetString(fieldName string) string {
	if ps.s1.HasField(fieldName) {
		return ps.s1.GetString(fieldName)
	}

	return ps.s2.GetString(fieldName)
}

// Returns a constant value from the current record.
func (ps *ProductScan) GetVal(fieldName string) *types.Constant {
	if ps.s1.HasField(fieldName) {
		return ps.s1.GetVal(fieldName)
	}

	return ps.s2.GetVal(fieldName)
}

func (ps *ProductScan) HasField(fieldName string) bool {
	return ps.s1.HasField(fieldName) || ps.s2.HasField(fieldName)
}

func (ps *ProductScan) Close() {
	ps.s1.Close()
	ps.s2.Close()
}
