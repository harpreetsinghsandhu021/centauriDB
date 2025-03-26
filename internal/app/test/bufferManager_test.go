package test

import (
	"centauri/internal/app/buffer"
	"centauri/internal/app/file"
	"centauri/internal/app/log"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func setupBufferManagerTest(t *testing.T) (*file.FileManager, *log.LogManager, func()) {
	dbDir := "./testdb"
	logdir := "./testlog"

	if err := os.MkdirAll(dbDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	if err := os.MkdirAll(logdir, 0755); err != nil {
		t.Fatalf("Failed to create log directory: %v", err)
	}

	fm, err := file.NewFileManager(dbDir, 400)
	if err != nil {
		t.Fatalf("Failed to create file manager: %v", err)
	}

	lm, err := log.NewLogManager(fm, "testlog")
	if err != nil {
		t.Fatalf("Failed to create log manager: %v", err)
	}

	cleanup := func() {
		os.RemoveAll(dbDir)
		os.RemoveAll(logdir)
	}

	return fm, lm, cleanup
}

func TestNewBufferManager(t *testing.T) {
	fm, lm, cleanup := setupBufferManagerTest(t)
	defer cleanup()

	numBuffs := 3
	bm := buffer.NewBufferManager(fm, lm, numBuffs)

	fmt.Println(bm.Available())

	if bm.Available() != numBuffs {
		t.Errorf("Expected %d available buffers, got %d", numBuffs, bm.Available())
	}
}

func TestBufferManager_PinUnpin(t *testing.T) {
	fm, lm, cleanup := setupBufferManagerTest(t)
	defer cleanup()

	fileName := "testfile"
	testFile, err := os.Create("./testdb/" + fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	testFile.Close()

	numBuffs := 3
	bm := buffer.NewBufferManager(fm, lm, numBuffs)
	block := file.NewBlockID(fileName, 1)

	// Test pinning a buffer
	buff, err := bm.Pin(block)

	if err != nil {
		t.Fatalf("Failed to pin buffer: %v", err)
	}

	if bm.Available() != numBuffs-1 {
		t.Errorf("Expected %d available buffers after pin, got %d", numBuffs, bm.Available())
	}

	bm.Unpin(buff)

	if bm.Available() != numBuffs {
		t.Errorf("Expected %d available buffers after pin, got %d", numBuffs, bm.Available())
	}
}

/*
Tests the buffer manager's behavior when attempting to pin a block
when all buffers are already pinned. It verifies that:
1. The buffer manager correctly handles the case when no buffers are available
2. Returns a BufferAbortError when attempting to pin a block with no available buffers
3. Buffer manager maintains its maximum buffer limit

Test setup:
- Creates 5 test files
- Initializes buffer manager with 2 buffers
- Pins all available buffers
- Attempts to pin an additional buffer

Expected outcome:
- Should return BufferAbortError when attempting to pin beyond available capacity
*/
func TestBufferManager_BufferAbortError(t *testing.T) {
	fm, lm, cleanup := setupBufferManagerTest(t)
	defer cleanup()

	// Create test files
	for i := 0; i < 5; i++ {
		fileName := fmt.Sprintf("testfile%d", i)
		testFile, err := os.Create("./testdb/" + fileName)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		testFile.Close()
	}

	// Create buffer manager with limited buffers
	numBuffs := 2
	bm := buffer.NewBufferManager(fm, lm, numBuffs)

	// Pin all available buffers
	pinned := make([]*buffer.Buffer, numBuffs)
	for i := 0; i < numBuffs; i++ {
		block := file.NewBlockID(fmt.Sprintf("testfile%d", i), 1)
		buff, err := bm.Pin(block)

		if err != nil {
			t.Fatalf("Failed to pin buffer %d: %v", i, err)
		}
		pinned[i] = buff
	}

	// Try to pin another buffer - should fail with timeout
	block := file.NewBlockID("testfile4", 1)
	_, err := bm.Pin(block)

	if err == nil {
		t.Error("Expected BufferAbortError, got nil")
	}

	if _, ok := err.(buffer.BufferAbortError); !ok {
		t.Errorf("Expected BufferAbortError, got %T", err)
	}

}

// Tests the FlushAll functionality of the BufferManager.
// It verifies that:
// 1. A buffer can be pinned and modified
// 2. The FlushAll operation successfully persists modified buffer contents to disk
// 3. The persisted data can be retrieved after flushing
//
// The test creates a temporary file, pins a buffer associated with a block in that file,
// modifies the buffer contents, and then calls FlushAll. It then verifies that the
// modifications were correctly written to disk by reading the data back from a newly
// pinned buffer.
func TestBufferManager_Flushall(t *testing.T) {
	fm, lm, cleanup := setupBufferManagerTest(t)
	defer cleanup()

	fileName := "testfile"
	testFile, err := os.Create("./testdb" + fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	testFile.Close()

	bm := buffer.NewBufferManager(fm, lm, 3)
	block := file.NewBlockID(fileName, 1)

	// Pin and modify a buffer
	buff, err := bm.Pin(block)
	if err != nil {
		t.Fatalf("Failed to pin buffer: %v", err)
	}

	// Modify buffer
	buff.Contents().SetString(0, "test data")
	buff.SetModified(1, 100)

	// Flush all buffers for transaction 1
	bm.FlushAll(1)

	// Verify data was persisted
	newBuff, err := bm.Pin(block)

	if err != nil {
		t.Fatalf("Failed to pin buffer after flush: %v", err)
	}

	if newBuff.Contents().GetString(0) != "test data" {
		t.Error("Data was not persisted after flush")
	}

}

// Tests the concurrent access capabilities of the BufferManager.
// It creates multiple test files and launches several goroutines that simultaneously attempt to
// pin and unpin blocks in the buffer pool. The test verifies that:
// - Multiple goroutines can safely access the buffer manager concurrently
// - The buffer manager maintains correct buffer availability count after concurrent operations
// - The buffer pool size remains consistent after all operations complete
// The test uses a buffer pool size of 3 and launches 5 concurrent goroutines.
func TestBufferManager_ConcurrentAccess(t *testing.T) {
	fm, lm, cleanup := setupBufferManagerTest(t)
	defer cleanup()

	for i := 0; i < 5; i++ {
		fileName := fmt.Sprintf("testfile%d", i)
		testFile, err := os.Create("./testdb/" + fileName)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		testFile.Close()
	}

	bm := buffer.NewBufferManager(fm, lm, 3)
	// WaitGroup to ensure all goroutines complete before test finishes
	var wg sync.WaitGroup
	// Number of concurrent goroutines to launch
	numGoroutines := 5

	// Launch multiple goroutines to test concurrent buffer manager access
	for i := 0; i < numGoroutines; i++ {
		// Increment WaitGroup counter before launching each goroutine
		wg.Add(1)

		// Launch goroutine with immediately invoked function expression
		go func(id int) {
			// Decrement WaitGroup counter when goroutine completes
			defer wg.Done()

			// Create a unique block ID for each goroutine
			block := file.NewBlockID(fmt.Sprintf("testfile%d", id), 1)
			// Attempt to pin the block, may fail if no buffers available
			buff, err := bm.Pin(block)

			if err == nil {
				// Simulate some work being done with the buffer
				time.Sleep(100 * time.Millisecond)
				// Release the buffer back to the pool
				bm.Unpin(buff)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify that all buffers were correctly unpinned and returned to available state
	if bm.Available() != 3 {
		t.Errorf("Expected 3 available buffers afteer concurrent access, got %d", bm.Available())
	}

}
