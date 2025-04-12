package materialize

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/record"
	"centauri/internal/app/types"
)

// Implements a merge sort algorithmn over 1 or 2 sorted runs.
// It produces a sorted view of records by comparing and merging records from
// the underlying scans according to the specified comparator.
// Key Characterstics:
// - Merges 1 or 2 sorted input streams
// - Maintains sort order via RecordComparator
// - Supports position saving/restoration
// - Implements the Scan interface for transparent usage
type SortScan struct {
	interfaces.Scan
	s1            *record.TableScan // First sorted input scan
	s2            *record.TableScan // Second sorted input scan
	currentScan   *record.TableScan // Currently active scan (s1 or s2)
	comp          *RecordComparator // Comparator for determining sort order
	hasMore1      bool
	hasMore2      bool
	savedPosition []*types.RID // Saved positions for restoration
}

func NewSortScan(runs []*TempTable, comp *RecordComparator) interfaces.Scan {
	// Initialize first run
	s1 := runs[0].Open()
	hasMore1 := s1.Next()

	// Initialize second run if present
	var s2 *record.TableScan
	var hasMore2 bool

	if len(runs) > 1 {
		s2 = runs[1].Open()
		hasMore2 = s2.Next()
	}

	return &SortScan{
		s1:       s1,
		s2:       s2,
		comp:     comp,
		hasMore1: hasMore1,
		hasMore2: hasMore2,
	}
}

// Resets the scan to its initial state:
// 1. Clears current scan position
// 2. Resets both input scans to their first records
// 3. Prepares for new merge iteration
func (ss *SortScan) BeforeFirst() {
	ss.currentScan = nil
	ss.s1.BeforeFirst()
	ss.hasMore1 = ss.s1.Next()
	if ss.s2 != nil {
		ss.s2.BeforeFirst()
		ss.hasMore2 = ss.s2.Next()
	}
}

// Advances to the next record in sorted order by:
// 1. Advancing the current scan if set
// 2. Selecting the next smallest record bw both scans
// 3. Setting the chosen scan as current
func (ss *SortScan) Next() bool {
	// Advance the current scan if set
	if ss.currentScan != nil {
		if ss.currentScan == ss.s1 {
			ss.hasMore1 = ss.s1.Next()
		} else if ss.currentScan == ss.s2 {
			ss.hasMore2 = ss.s2.Next()
		}
	}

	// Determine next record source
	if !ss.hasMore1 && !ss.hasMore2 {
		return false // No more records
	} else if ss.hasMore1 && ss.hasMore2 {
		// Both have records - compare to find smaller
		if ss.comp.Compare(ss.s1, ss.s2) < 0 {
			ss.currentScan = ss.s1
		} else {
			ss.currentScan = ss.s2
		}
	} else if ss.hasMore1 {
		ss.currentScan = ss.s1
	} else if ss.hasMore2 {
		ss.currentScan = ss.s2
	}

	return true
}

// Closes both underlying scans
func (ss *SortScan) Close() {
	ss.s1.Close()
	if ss.s2 != nil {
		ss.s2.Close()
	}
}

func (ss *SortScan) GetVal(fldname string) *types.Constant {
	return ss.currentScan.GetVal(fldname)
}

func (ss *SortScan) GetInt(fldname string) int {
	return ss.currentScan.GetInt(fldname)
}

func (ss *SortScan) GetString(fldname string) string {
	return ss.currentScan.GetString(fldname)
}

func (ss *SortScan) HasField(fldname string) bool {
	return ss.currentScan.HasField(fldname)
}

// Captures the current positions of both scans for later restoration.
// Useful for nested loop operations that need to reset their state.
func (ss *SortScan) SavePosition() {
	rid1 := ss.s1.GetRID()
	var rid2 *types.RID
	if ss.s2 != nil {
		rid2 = ss.s2.GetRID()
	}

	ss.savedPosition = []*types.RID{rid1, rid2}
}

// Resets both scans to their previously saved positions.
// Must be preceded by a call to SavePosition()
func (ss *SortScan) RestorePosition() {
	if ss.savedPosition == nil {
		panic("no saved position")
	}

	rid1 := ss.savedPosition[0]
	rid2 := ss.savedPosition[1]

	ss.s1.MoveToRID(rid1)
	if ss.s2 != nil && rid2 != nil {
		ss.s2.MoveToRID(rid2)
	}

	// Reset current scan tracking
	ss.currentScan = nil
	ss.hasMore1 = true
	if ss.s2 != nil {
		ss.hasMore2 = true
	}
}
