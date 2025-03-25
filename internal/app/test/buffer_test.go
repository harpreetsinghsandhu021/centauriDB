package test

import (
	"centauri/internal/app/buffer"
	"centauri/internal/app/file"
	"centauri/internal/app/log"
	"os"
	"testing"
)

// testSetup creates temporary directories and managers for testing
func testSetup(t *testing.T) (*file.FileManager, *log.LogManager, func()) {
	// Create temporary directories
	dbDir := "./testdb"
	logDir := "./testlog"

	// Create directories
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatalf("Failed to create log directory: %v", err)
	}

	// Initialize managers
	fm, err := file.NewFileManager(dbDir, 400)
	if err != nil {
		t.Fatalf("Failed to create FileManager: %v", err)
	}
	lm, err := log.NewLogManager(fm, "testlog")
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		os.RemoveAll(dbDir)
		os.RemoveAll(logDir)
	}

	return fm, lm, cleanup
}

// Verifies the initial state of a newly created buffer.
// It checks that:
// - The buffer is not pinned (pin count is 0)
// - The modifying transaction number is -1
// - The block reference is nil
func TestBuffer_NewBuffer(t *testing.T) {
	// Set up test environment with file and log managers
	fm, lm, cleanup := testSetup(t)
	defer cleanup()

	buffer := buffer.NewBuffer(fm, lm)

	if buffer.IsPinned() {
		t.Errorf("New buffer should have 0 pins, got found")
	}
	if buffer.ModifyingTx() != -1 {
		t.Errorf("New buffer should have txnum -1, got %d", buffer.ModifyingTx())
	}
	if buffer.Block() != nil {
		t.Error("New buffer should have nil block")
	}
}

// Tests the Pin and Unpin functionality of a buffer.
// It verifies:
// - A buffer can be pinned and its pinned status is correctly reported
// - Multiple pins increase the pin count appropriately
// - Unpinning decrements the pin count correctly
// - Buffer remains pinned until all pins are removed
// - Buffer's pinned status is false when all pins are removed
func Test_PinUnpin(t *testing.T) {
	// Set up test environment with file and log managers
	fm, lm, cleanup := testSetup(t)
	defer cleanup()

	buffer := buffer.NewBuffer(fm, lm)

	// Test Pin
	buffer.Pin()

	if !buffer.IsPinned() {
		t.Error("Buffer should be pinned after Pin()")
	}

	if buffer.Pins() != 1 {
		t.Errorf("Expected pins to be 1, got %d", buffer.Pins())
	}

	// Test multiple pins
	buffer.Pin()

	if buffer.Pins() != 2 {
		t.Errorf("Expected pins to be 2, got %d", buffer.Pins())
	}

	// Test Unpin
	buffer.Unpin()

	if buffer.Pins() != 1 {
		t.Errorf("Expected pins to be 1, got %d", buffer.Pins())
	}

	if !buffer.IsPinned() {
		t.Error("Buffer should still be pinned")
	}

	buffer.Unpin()

	if buffer.IsPinned() {
		t.Error("Buffer should not be pinned")
	}
}

// Tests the functionality of assigning blocks to a buffer
// and verifies the buffer's state changes during block assignments
func TestBuffer_AssignToBlock(t *testing.T) {
	// Set up test environment with file and log managers
	fm, lm, cleanup := testSetup(t)
	defer cleanup()

	// Create a new buffer instance
	buffer := buffer.NewBuffer(fm, lm)

	// Create first test file for block assignment
	filename := "testfile"
	testFile, err := os.Create("./testdb/" + filename)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	testFile.Close()

	// Create a block ID for the first file
	block := file.NewBlockID(filename, 1)

	// Pin buffer and assign it to the first block
	buffer.Pin()
	buffer.AssignToBlock(block)

	// Simulate data modification in the buffer
	page := buffer.Contents()
	page.SetString(0, "test data")
	buffer.SetModified(1, 1) // Mark as modified with transaction ID 1 and LSN 1

	// Release the buffer
	buffer.Unpin()

	// Create second test file for new block assignment
	newFilename := "testfile2"
	newFile, err := os.Create("./testdb/" + newFilename)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	newFile.Close()

	// Create a new block ID and reassign buffer
	newBlock := file.NewBlockID(newFilename, 1)
	buffer.AssignToBlock(newBlock)

	// Verify buffer state after reassignment
	if buffer.Block() != newBlock {
		t.Error("Buffer should be assigned to the new block")
	}

	// Verify pins are reset after reassignment
	if buffer.Pins() != 0 {
		t.Errorf("Pins should be reset to 0, got %d", buffer.Pins())
	}

	// Verify modification status is reset
	if buffer.ModifyingTx() != -1 {
		t.Error("Modified flag should be reset")
	}
}

// TestBuffer_SetModified verifies the buffer's modification tracking functionality.
// It tests if:
// - The modifying transaction ID is correctly set and retrieved
// - The Last Sequence Number (LSN) is properly maintained when modified
func TestBuffer_SetModified(t *testing.T) {
	fm, lm, cleanup := testSetup(t)
	defer cleanup()

	buffer := buffer.NewBuffer(fm, lm)

	buffer.SetModified(1, 100)

	if buffer.ModifyingTx() != 1 {
		t.Errorf("Expecting modifying tx to be 1, got %d", buffer.ModifyingTx())
	}

	if buffer.LastSequenceNumber() != 100 {
		t.Errorf("LSN shouldn not change when negative, got %d", buffer.LastSequenceNumber())
	}
}

func TestBuffer_Flush(t *testing.T) {
	fm, lm, cleanup := testSetup(t)
	defer cleanup()

	buf := buffer.NewBuffer(fm, lm)

	filename := "testfile"
	testFile, err := os.Create("./testdb/" + filename)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	testFile.Close()

	block := file.NewBlockID(filename, 1)
	buf.AssignToBlock(block)

	page := buf.Contents()
	page.SetString(0, "test data")
	buf.SetModified(1, 100)

	buf.Flush()

	if buf.ModifyingTx() != -1 {
		t.Error("Buffer should not be marked as modified after flush")
	}

	// Verify data persistence
	newBuf := buffer.NewBuffer(fm, lm)
	newBuf.AssignToBlock(block)
	if newBuf.Contents().GetString(0) != "test data" {
		t.Error("Data was not persisted after flush")
	}

}
