package test

import (
	"centauri/internal/app/file"
	"centauri/internal/app/tx"
	"sync"
	"testing"
)

func TestNewLockTable(t *testing.T) {
	lt := tx.NewLockTable()
	if lt == nil {
		t.Error("NewLockTable returned nil")
	}

	if lt == nil {
		t.Error("locks map was not initialized")
	}

	if lt.GetLocks() == nil {
		t.Error("locks map was not initialized")
	}

}

func TestSLock(t *testing.T) {
	lt := tx.NewLockTable()
	block := file.NewBlockID("test.db", 1)

	t.Run("Basic SLock", func(t *testing.T) {
		err := lt.SLock(*block)

		if err != nil {
			t.Errorf("Failed to acquite SLock: %v", err)
		}

		if lt.GetLockVal(*block) != 1 {
			t.Errorf("Expected lock value 2, got %d", lt.GetLockVal(*block))
		}

	})

	t.Run("Mutiple SLocks", func(t *testing.T) {
		err := lt.SLock(*block)
		if err != nil {
			t.Errorf("Failed to acquire second SLock: %v", err)
		}

		if lt.GetLockVal(*block) != 2 {
			t.Errorf("Expected lock value 2, got %d", lt.GetLockVal(*block))
		}

	})

	t.Run("SLock Timeout with XLock", func(t *testing.T) {
		block2 := file.NewBlockID("test.db", 2)

		// First acquire an XLock
		err := lt.XLock(*block2)
		if err != nil {
			t.Errorf("Failed to acquire XLock: %v", err)
		}

		err = lt.SLock(*block2)

		if err != tx.ErrLockTimeout {
			t.Errorf("Expected timeout error, got %v", err)
		}
	})
}

func TestXLock(t *testing.T) {
	lt := tx.NewLockTable()
	block := file.NewBlockID("test.db", 1)

	t.Run("Basic XLock", func(t *testing.T) {
		err := lt.XLock(*block)
		if err != nil {
			t.Errorf("Failed to acquire XLock: %v", err)
		}

		if lt.GetLockVal(*block) != -1 {
			t.Errorf("Expected lock value -1, got %d", lt.GetLockVal(*block))
		}
	})

	t.Run("XLock Timeout with SLock", func(t *testing.T) {
		block2 := file.NewBlockID("test.db", 2)
		err := lt.SLock(*block2)
		if err != nil {
			t.Errorf("Failed to acquire SLock: %v", err)
		}

		err = lt.XLock(*block2)

		if err != tx.ErrLockTimeout {
			t.Errorf("Expected timeout error, got %v", err)
		}
	})
}

func TestUnlock(t *testing.T) {
	lt := tx.NewLockTable()
	block := file.NewBlockID("test.db", 1)

	t.Run("Unlock Single SLock", func(t *testing.T) {
		err := lt.SLock(*block)

		if err != nil {
			t.Errorf("Failed to acquire SLock: %v", err)
		}

		lt.Unlock(*block)

		if lt.GetLockVal(*block) != 0 {
			t.Errorf("Expected lock value 0 got %d", lt.GetLockVal(*block))
		}
	})

	t.Run("Unlock Multiple SLocks", func(t *testing.T) {
		err := lt.SLock(*block)

		if err != nil {
			t.Errorf("Failed to acquire first SLock: %v", err)
		}

		err = lt.SLock(*block)
		if err != nil {
			t.Errorf("Failed to acquire second SLock")
		}

		lt.Unlock(*block)
		if lt.GetLockVal(*block) != 1 {
			t.Errorf("Expected lock value 1, got %d", lt.GetLockVal(*block))
		}
	})

	t.Run("Unlock XLock", func(t *testing.T) {
		block2 := file.NewBlockID("test.db", 2)
		err := lt.XLock(*block2)
		if err != nil {
			t.Errorf("Failed to acquire XLock: %v", err)
		}

		lt.Unlock(*block2)
		if lt.GetLockVal(*block2) != 0 {
			t.Errorf("Expectef lock value 0, got %d", lt.GetLockVal(*block2))
		}
	})

}

func TestConcurrency(t *testing.T) {
	lt := tx.NewLockTable()
	block := file.NewBlockID("test.db", 1)
	const numGorRoutines = 10

	// Test concurrent SLocks
	t.Run("Concurrent SLocks", func(t *testing.T) {
		var wg sync.WaitGroup

		for i := 0; i < numGorRoutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := lt.SLock(*block)
				if err != nil {
					t.Errorf("Failed to acquire concurrent SLock: %v", err)
				}
			}()
		}

		wg.Wait()

		if lt.GetLockVal(*block) != numGorRoutines {
			t.Errorf("Expected %d Slocks, got %d", numGorRoutines, lt.GetLockVal(*block))
		}
	})

	// Test concurrent unlocks
	t.Run("Concurrent Unlocks", func(t *testing.T) {
		var wg sync.WaitGroup

		for i := 0; i < numGorRoutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				lt.Unlock(*block)
			}()
		}
		wg.Wait()

		if lt.GetLockVal(*block) != 0 {
			t.Errorf("Expected 0 locks after concurrent unlocks, got %d", lt.GetLockVal(*block))
		}
	})

	t.Run("XLock Prevents Concurrent Access", func(t *testing.T) {
		err := lt.XLock(*block)
		if err != nil {
			t.Errorf("Failed to acquire XLock: %v", err)
		}

		var wg sync.WaitGroup
		errorChan := make(chan error, numGorRoutines)

		for i := 0; i < numGorRoutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := lt.SLock(*block)
				if err != nil && err != tx.ErrLockTimeout {
					errorChan <- err
				}
			}()
		}

		wg.Wait()
		close(errorChan)

		for err := range errorChan {
			t.Errorf("Unexpted error during concurrent access: %v", err)
		}
	})

}
