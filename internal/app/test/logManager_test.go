package test

import (
	"bytes"
	"centauri/internal/app/file"
	"centauri/internal/app/log"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// Test setup helper

func setupTest(t *testing.T) (*log.LogManager, string, func()) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "logmanager_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Initialize FileManager
	fm, err := file.NewFileManager(tempDir, 400)
	if err != nil {
		t.Fatalf("failed to create file manager: %v", err)
	}

	// Create LogManager
	logFile := "test.log"
	lm, err := log.NewLogManager(fm, logFile)
	if err != nil {
		t.Fatalf("failed to create log manager: %v", err)
	}

	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return lm, logFile, cleanup
}

func TestLogManager_Append(t *testing.T) {
	lm, _, cleanup := setupTest(t)
	defer cleanup()

	tests := []struct {
		name    string
		record  []byte
		wantErr bool
	}{
		{
			name:    "simple append",
			record:  []byte("test record"),
			wantErr: false,
		},
		{
			name:    "empty record",
			record:  []byte{},
			wantErr: false,
		},
		{
			name:    "large record",
			record:  bytes.Repeat([]byte("a"), 350),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lsn, err := lm.Append(tt.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("Append() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && lsn <= 0 {
				t.Errorf("Append() returned invalid LSN = %d", lsn)
			}
		})
	}
}

func TestLogManager_FlushAndIterator(t *testing.T) {
	lm, _, cleanup := setupTest(t)
	defer cleanup()

	// Create test records
	testRecords := [][]byte{
		[]byte("record1"),
		[]byte("record2"),
		[]byte("record3"),
	}

	// Append records and collect LSNs
	var lastLSN int
	for _, record := range testRecords {
		lsn, err := lm.Append(record)
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
		lastLSN = lsn
	}

	// Test Flush
	if err := lm.Flush(lastLSN); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Test Iterator
	iter, err := lm.Iterator()
	if err != nil {
		t.Fatalf("Iterator() error = %v", err)
	}

	// Verify records in reverse order (as per implementation)
	count := len(testRecords) - 1
	for iter.HasNext() {
		record, err := iter.Next()
		if err != nil {
			t.Fatalf("Iterator.Next() error = %v", err)
		}

		if !bytes.Equal(record, testRecords[count]) {
			t.Errorf("Iterator.Next() = %v, want %v", record, testRecords[count])
		}
		count--
	}

	if count != -1 {
		t.Errorf("Iterator didn't return all records, remaining: %d", count+1)
	}
}
func TestLogManager_ConcurrentOperations(t *testing.T) {
	// Setup test environment and ensure cleanup
	lm, _, cleanup := setupTest(t)
	defer cleanup()

	// Configuration for concurrent operations
	const numGoroutines = 3       // Number of concurrent writers
	const recordsPerGoroutine = 5 // Records each goroutine will write

	// Create a buffered channel for collecting errors from goroutines
	// Buffer size equals total possible operations to prevent blocking
	errChan := make(chan error, numGoroutines*recordsPerGoroutine)
	var wg sync.WaitGroup // Tracks completion of all goroutines

	// Create context with timeout to prevent test from hanging
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel() // Ensure context is cancelled when test completes

	// Launch multiple goroutines to simulate concurrent access
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1) // Increment WaitGroup counter before launchig goroutine
		go func(routineID int) {
			defer func() {
				wg.Done() // Ensure Waitgroup is decremented
				// Recover from potential panics
				if r := recover(); r != nil {
					errChan <- fmt.Errorf("goroutine %d panicked: %v", routineID, r)
				}
			}()

			// Each goroutine writes multiple records
			for j := 0; j < recordsPerGoroutine; j++ {
				select {
				case <-ctx.Done():
					return // Exit without sending error to prevent deadlock
				default:
					// Create unique record for this goroutine and iteration
					record := []byte(fmt.Sprintf("test-%d-%d", routineID, j))

					// Attempt to append the record
					// Use non-blocking send to error channel
					if lsn, err := lm.Append(record); err != nil {
						select {
						case errChan <- fmt.Errorf("append error in routine %d: %v", routineID, err):
						default:
							// If channel is full, just continue
						}
						return
					} else if err := lm.Flush(lsn); err != nil {
						select {
						case errChan <- fmt.Errorf("flush error in routine %d: %v", routineID, err):
						default:
							// If channel is full, just continue
						}
						return
					}
				}
			}
		}(i)
	}

	// Create channel to signal completion of all goroutines
	done := make(chan struct{})
	go func() {
		wg.Wait()   // Wait for all goroutines to complete
		close(done) // Signal completion by closing channel
	}()

	// Wait for either completion or timeout
	select {
	case <-ctx.Done():
		t.Fatal("Test timed out")
		return
	case <-done:
		// Continue with verification

	}

	// Verify results
	iter, err := lm.Iterator()
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}

	count := 0
	for iter.HasNext() {
		if _, err := iter.Next(); err != nil {
			t.Fatalf("Failed to read record: %v", err)
		}
		count++
	}

	// Check final count
	expected := numGoroutines * recordsPerGoroutine
	if count != expected {
		t.Errorf("Record count mismatch: got %d, want %d", count, expected)
	}

	// Check for any errors that occurred
	close(errChan)
	for err := range errChan {
		t.Error(err)
	}
}
