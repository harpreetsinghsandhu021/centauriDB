package query

import (
	"centauri/internal/app/index"
	"centauri/internal/app/interfaces"
	"centauri/internal/app/record"
	"centauri/internal/app/types"
)

// Implements a scan for index join operations.
// It's similar to a product scan but uses an index for efficient joining.
type IndexJoinScan struct {
	lhs       interfaces.Scan
	idx       index.Index
	joinField string
	rhs       *record.TableScan
}

func NewIndexJoinScan(lhs interfaces.Scan, idx index.Index, joinField string, rhs *record.TableScan) *IndexJoinScan {
	ijs := &IndexJoinScan{
		lhs:       lhs,
		idx:       idx,
		joinField: joinField,
		rhs:       rhs,
	}
	ijs.BeforeFirst()

	return ijs
}

// Positions the scan before the first record.
// Positions the LHS scan at its first record and the index before the first join value.
func (ijs *IndexJoinScan) BeforeFirst() {
	ijs.lhs.BeforeFirst()
	ijs.lhs.Next()
	ijs.resetIndex()
}

// Moves the scan to the next record.
// Returns false if there are no more records to scan.
func (ijs *IndexJoinScan) Next() bool {

	for {
		if ijs.idx.Next() {
			ijs.rhs.MoveToRID(ijs.idx.GetDataRid())
			return true
		}
		if !ijs.lhs.Next() {
			return false
		}

		ijs.resetIndex()
	}
}

// Returns the integer value of the specified field.
func (ijs *IndexJoinScan) GetInt(fldName string) int {
	if ijs.rhs.HasField(fldName) {
		return ijs.rhs.GetInt(fldName)
	}
	return ijs.lhs.GetInt(fldName)
}

func (ijs *IndexJoinScan) GetVal(fldName string) *types.Constant {
	if ijs.rhs.HasField(fldName) {
		return ijs.rhs.GetVal(fldName)
	}
	return ijs.lhs.GetVal(fldName)
}

func (ijs *IndexJoinScan) GetString(fldName string) string {
	if ijs.rhs.HasField(fldName) {
		return ijs.rhs.GetString(fldName)
	}
	return ijs.lhs.GetString(fldName)
}

func (ijs *IndexJoinScan) HasField(fldName string) bool {
	return ijs.rhs.HasField(fldName) || ijs.lhs.HasField(fldName)
}

func (ijs *IndexJoinScan) Close() {
	ijs.lhs.Close()
	ijs.idx.Close()
	ijs.rhs.Close()
}

func (ijs *IndexJoinScan) resetIndex() {
	searchKey := ijs.lhs.GetVal(ijs.joinField)
	ijs.idx.BeforeFirst(searchKey)
}
