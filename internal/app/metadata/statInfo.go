package metadata

// Holds three pieces of statistical information about a table
// the no of blocks, the number of records and number of distinct values for each field
type StatInfo struct {
	numBlocks int
	numRecs   int
}

func NewStatInfo(numBlocks int, numRecs int) *StatInfo {
	return &StatInfo{
		numBlocks: numBlocks,
		numRecs:   numRecs,
	}
}

func (si *StatInfo) BlocksAccessed() int {
	return si.numBlocks
}

func (si *StatInfo) RecordsOutput() int {
	return si.numRecs
}

// This is wrong
func (si *StatInfo) DistinctValues(fieldname string) int {
	return 1 + (si.numRecs / 3)
}
