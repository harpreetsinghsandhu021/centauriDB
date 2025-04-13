package multibuffer

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/query"
	"centauri/internal/app/record"
	"centauri/internal/app/tx"
	"centauri/internal/app/types"
)

// Implements the Scan interface for the multi-buffer version of the product operator.
// It efficiently processes joined data in chunks.
type MultibufferProductScan struct {
	interfaces.Scan
	tx           *tx.Transaction
	lhsscan      interfaces.Scan
	rhsscan      interfaces.Scan
	prodscan     interfaces.Scan
	fileName     string
	layout       *record.Layout
	chunkSize    int
	nextBlockNum int
	fileSize     int
}

func NewMultiBufferProductScan(tx *tx.Transaction, lhsscan interfaces.Scan, tableName string, layout *record.Layout) interfaces.Scan {
	fileName := tableName + ".tbl"
	size, _ := tx.Size(fileName)
	mps := &MultibufferProductScan{
		tx:       tx,
		lhsscan:  lhsscan,
		fileName: fileName,
		layout:   layout,
		fileSize: size,
		rhsscan:  nil,
		prodscan: nil,
	}

	// Calculate the optimal chunk size based on available buffers
	available := tx.AvailableBuffers()
	mps.chunkSize = BestFactor(available, mps.fileSize)

	mps.BeforeFirst()
	return mps
}

// Positions the scan before the first record.
// The LHS scan is positioned at its first record,
// and the RHS scan is positioned before the first record of the first chunk.
func (mps *MultibufferProductScan) BeforeFirst() {
	mps.nextBlockNum = 0
	mps.UseNextChunk()
}

// Moves to the next record in the current scan.
// If there are no more records in the current chunk,
// then move to the next LHS record and the beginning of that chunk.
// If there are no more LHS records, then move to the next chunk and begin again.
func (mps *MultibufferProductScan) Next() bool {
	for !mps.prodscan.Next() {
		if !mps.UseNextChunk() {
			return false
		}
	}
	return true
}

func (mps *MultibufferProductScan) Close() {
	if mps.prodscan != nil {
		mps.prodscan.Close()
	}
}

func (mps *MultibufferProductScan) GetVal(fieldName string) *types.Constant {
	return mps.prodscan.GetVal(fieldName)
}

func (mps *MultibufferProductScan) GetInt(fieldName string) int {
	return mps.prodscan.GetInt(fieldName)
}

func (mps *MultibufferProductScan) GetString(fldname string) string {
	return mps.prodscan.GetString(fldname)
}

func (mps *MultibufferProductScan) HasField(fldname string) bool {
	return mps.prodscan.HasField(fldname)
}

// Sets up processing for the next chunk. It creates a new ChunkScan for the next chunk
// It creates a new ChunkScan for the next chunk of blocks from the RHS table, resets the
// LHS scan to its beginning, and creates a new ProductScan.
func (mps *MultibufferProductScan) UseNextChunk() bool {
	// Check if we've processed all blocks
	if mps.nextBlockNum >= mps.fileSize {
		return false
	}

	// Close the previous RHS if it exists
	if mps.rhsscan != nil {
		mps.rhsscan.Close()
	}

	// Calculate the end block for the chunk
	end := mps.nextBlockNum + mps.chunkSize - 1
	if end >= mps.fileSize {
		end = mps.fileSize - 1
	}

	// Create a new chunkScan for this range of blocks
	mps.rhsscan = NewChunkScan(mps.tx, mps.fileName, *mps.layout, mps.nextBlockNum, end)

	// Reset the LHS to its beginning
	mps.lhsscan.BeforeFirst()

	// Create a new ProductScan combining the LHS scan and the chunk scan
	mps.prodscan = query.NewProductScan(mps.lhsscan, mps.rhsscan)

	mps.nextBlockNum = end + 1
	return true
}
