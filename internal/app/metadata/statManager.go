package metadata

import (
	"centauri/internal/app/record"
	"centauri/internal/app/tx"
	"sync"
)

// Maintains statistics about the tables in the database.
// It provides thread-safe access to table statistics and automatically
// refreshes them periodically.
type StatManager struct {
	tm         *TableManager
	tableStats map[string]StatInfo
	numCalls   int
	mu         sync.Mutex
}

func NewStatManager(tm *TableManager, tx *tx.Transaction) *StatManager {
	sm := &StatManager{
		tm:         tm,
		tableStats: make(map[string]StatInfo),
	}

	sm.refreshStatistics(tx) // Initial load of statistics
	return sm
}

// Returns statistics for the specified table.
// If the statistics are not in cache or are stale, they are recalculated
func (sm *StatManager) GetStatInfo(tablename string, layout *record.Layout, tx *tx.Transaction) StatInfo {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if statistics need refresh
	sm.numCalls++
	if sm.numCalls > 100 {
		sm.refreshStatistics(tx)
	}

	// Get or calculate statistics
	si, exists := sm.tableStats[tablename]
	if !exists {
		si = sm.calcTableStats(tablename, layout, tx)
		sm.tableStats[tablename] = si
	}
	return si
}

// Recalculates statistics for all tables in the database.
// This is called periodically to ensure statistics remain current.
func (sm *StatManager) RefreshStatistics(tx *tx.Transaction) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.refreshStatistics(tx)
}

// The Internal implementation of statistics refresh.
func (sm *StatManager) refreshStatistics(tx *tx.Transaction) {
	// Reset statistics
	sm.tableStats = make(map[string]StatInfo)
	sm.numCalls = 0

	// Get table catalog layout
	tcatLayout := sm.tm.GetLayout("tblcat", tx)

	// Scan all tables in the catalog
	ts := record.NewTableScan(tx, "tblcat", tcatLayout)
	defer ts.Close()

	for ts.Next() {
		tableName := ts.GetString("tblname")
		layout := sm.tm.GetLayout(tableName, tx)
		stats := sm.calcTableStats(tableName, layout, tx)
		sm.tableStats[tableName] = stats
	}

}

// Calculates statistics for a single table
func (sm *StatManager) calcTableStats(tablename string, layout *record.Layout, tx *tx.Transaction) StatInfo {
	numRecs := 0
	numBlocks := 0

	// Scan the entire table
	ts := record.NewTableScan(tx, tablename, layout)
	defer ts.Close()

	for ts.Next() {
		numRecs++
		rid := ts.GetRID()

		if rid.BlockNumber()+1 > numBlocks {
			numBlocks = rid.BlockNumber() + 1
		}
	}

	return *NewStatInfo(numBlocks, numRecs)
}
