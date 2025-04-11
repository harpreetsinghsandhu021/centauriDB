package query

import (
	"centauri/internal/app/index"
	"centauri/internal/app/interfaces"
	"centauri/internal/app/record"
	"centauri/internal/app/types"
)

// Represents a scan for index selection operations.
// It implements the scan interface for indexed selection queries
type IndexSelectScan struct {
	interfaces.Scan
	ts  *record.TableScan
	idx index.Index
	val types.Constant
}

func NewIndexSelectScan(ts *record.TableScan, idx index.Index, val types.Constant) *IndexSelectScan {
	scan := &IndexSelectScan{
		ts:  ts,
		idx: idx,
		val: val,
	}

	scan.BeforeFirst()
	return scan
}

// Positions the scan before the first record, which means positioning the index before the first instance of the selection constant.
func (iss *IndexSelectScan) BeforeFirst() {
	iss.idx.BeforeFirst(&iss.val)
}

// Moves to the next record, which means moving the index to the next
// record satisfying the selection constant. Returns false if there are
// no more such index records. If successful, moves the table scan to the
// corresponding data record.
func (iss *IndexSelectScan) Next() bool {
	ok := iss.idx.Next()
	if ok {
		rid := iss.idx.GetDataRid()
		iss.ts.MoveToRID(rid)
	}
	return ok
}

// Returns the integer value of the specified field from the current data record.
func (iss *IndexSelectScan) GetInt(fldName string) int {
	return iss.ts.GetInt(fldName)
}

func (iss *IndexSelectScan) GetString(fldName string) string {
	return iss.ts.GetString(fldName)
}

func (iss *IndexSelectScan) GetVal(fldName string) types.Constant {
	return *iss.ts.GetVal(fldName)
}

func (iss *IndexSelectScan) HasField(fldName string) bool {
	return iss.ts.HasField(fldName)
}

// Closes the scan by closing both the index and the table scan.
func (iss *IndexSelectScan) Close() {
	iss.idx.Close()
	iss.ts.Close()
}
