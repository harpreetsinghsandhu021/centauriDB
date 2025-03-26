package test

import (
	"centauri/internal/app/file"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func setupTestDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "filemanager_test_*")
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	return dir
}

func cleanupTestDir(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		t.Errorf("Failed to cleanup test directory: %v", err)
	}
}

func TestNewFileManager(t *testing.T) {
	tests := []struct {
		name      string
		dir       string
		blockSize int
		wantErr   bool
	}{
		{
			name:      "Valid new directory",
			dir:       "testdb_new",
			blockSize: 400,
			wantErr:   false,
		},
		{
			name:      "Invalid block size",
			dir:       "testdb_invalid",
			blockSize: 0,
			wantErr:   false, // Current implementation doesn't validate block size
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDir := setupTestDir(t)
			defer cleanupTestDir(t, testDir)

			dbPath := filepath.Join(testDir, tt.dir)
			fm, err := file.NewFileManager(dbPath, tt.blockSize)

			if (err != nil) != tt.wantErr {
				t.Errorf("NewFileManager() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if fm != nil {
				if !fm.IsNew() {
					t.Error("Expected IsNew() to be true for new directory")
				}
				if fm.BlockSize() != tt.blockSize {
					t.Errorf("BlockSize() = %v, want %v", fm.BlockSize(), tt.blockSize)
				}
			}
		})
	}
}

func TestFileManager_ReadWrite(t *testing.T) {
	testDir := setupTestDir(t)
	defer cleanupTestDir(t, testDir)

	blockSize := 400
	fm, err := file.NewFileManager(testDir, blockSize)
	if err != nil {
		t.Fatalf("Failed to create FileManager: %v", err)
	}
	defer fm.Close()

	// Create test data
	testData := make([]byte, blockSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Create a page with test data
	page := file.NewPageFromBytes(testData)

	// Test writing and reading back
	blockID := file.NewBlockID("test.db", 0)

	// Test Write
	if err := fm.Write(blockID, page); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Test Read
	readPage := file.NewPageFromBytes(make([]byte, blockSize))
	if err := fm.Read(blockID, readPage); err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// Compare written and read data
	for i := 0; i < blockSize; i++ {
		if page.Contents()[i] != readPage.Contents()[i] {
			t.Errorf("Data mismatch at position %d: got %v, want %v",
				i, readPage.Contents()[i], page.Contents()[i])
		}
	}
}

func TestFileManager_Append(t *testing.T) {
	testDir := setupTestDir(t)
	defer cleanupTestDir(t, testDir)

	fm, err := file.NewFileManager(testDir, 400)
	if err != nil {
		t.Fatalf("Failed to create FileManager: %v", err)
	}
	defer fm.Close()

	filename := "test.db"

	// Test multiple appends
	for i := 0; i < 3; i++ {
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		if blk.Number() != i {
			t.Errorf("Block number = %d, want %d", blk.Number(), i)
		}
	}

	// Verify length
	length, err := fm.Length(filename)
	if err != nil {
		t.Fatalf("Length check failed: %v", err)
	}
	if length != 3 {
		t.Errorf("File length = %d, want 3", length)
	}
}

func TestFileManager_Length(t *testing.T) {
	testDir := setupTestDir(t)
	defer cleanupTestDir(t, testDir)

	fm, err := file.NewFileManager(testDir, 400)
	if err != nil {
		t.Fatalf("Failed to create FileManager: %v", err)
	}
	defer fm.Close()

	filename := "test.db"

	// Test empty file
	length, err := fm.Length(filename)
	if err != nil {
		t.Fatalf("Length check failed: %v", err)
	}
	if length != 0 {
		t.Errorf("Initial file length = %d, want 0", length)
	}

	// Add a block and test again
	_, err = fm.Append(filename)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	length, err = fm.Length(filename)
	if err != nil {
		t.Fatalf("Length check failed: %v", err)
	}
	if length != 1 {
		t.Errorf("File length after append = %d, want 1", length)
	}
}

func TestFileManager_ConcurrentAccess(t *testing.T) {
	testDir := setupTestDir(t)
	defer cleanupTestDir(t, testDir)

	fm, err := file.NewFileManager(testDir, 400)
	if err != nil {
		t.Fatalf("Failed to create FileManager: %v", err)
	}
	defer fm.Close()

	const numGoroutines = 10
	done := make(chan bool)

	for i := 0; i < numGoroutines; i++ {
		go func(n int) {
			filename := fmt.Sprintf("test%d.db", n)
			_, err := fm.Append(filename)
			if err != nil {
				t.Errorf("Concurrent append failed: %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}
