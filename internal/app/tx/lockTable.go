package tx

import (
	"centauri/internal/app/file"
	"errors"
	"sync"
	"time"
)

// Indicates that a lock acquistion failed due to timeout
var LockAbortError = errors.New("lock acquistion timed out")

// Defines the maximum time to wait for a lock
const MaxWaitTime = 10 * time.Second

// Manages locks on blocks for concurrent transactions
// - Negative values (-1) indicate an exclusive lock (XLock)
// - Positive values (>0) indicate the number of shared locks (SLock)
// - Zero indicates no locks
type LockTable struct {
	locks map[*file.BlockID]int
	mu    sync.Mutex // Protects the locks map and serves as mutex for the condition variable
	cond  *sync.Cond // For wait/notify system
}

func NewLockTable() *LockTable {
	lt := &LockTable{
		locks: make(map[*file.BlockID]int),
	}
	lt.cond = sync.NewCond(&lt.mu)
	return lt
}

// Acquires a shared lock on the specified block. If an exclusive lock exists, the goroutine will wait until
// the lock is released or MaxWaitTime is exceeded.
func (lt *LockTable) SLock(block *file.BlockID) error {
	// Acquire the lock table's mutex to ensure thread-safe access
	lt.mu.Lock()
	// Ensure mutex is released when function exits
	defer lt.mu.Unlock()

	startTime := time.Now()

	// Wait if there's an exclusive lock on the block
	for lt.hasXLock(block) {
		// Check if we've waited too long
		if time.Since(startTime) >= MaxWaitTime {
			return LockAbortError
		}

		// Set a timeout for this wait iteration
		remainingTime := MaxWaitTime - time.Since(startTime)

		// Create a channel to signal when the condition variable is notified
		waitCh := make(chan struct{})

		// Start a goroutine to wait on the condition variable
		// This allows us to implement a timeout mechanism while waiting
		go func() {
			// Wait for notification from another theread releasing a lock
			// This atomically releases the mutex and blocks until signaled
			lt.cond.L.Lock()
			lt.cond.Wait()
			lt.cond.L.Unlock()
			// Signal the main goroutine that we've been woken up
			close(waitCh)
		}()

		// Temporarily release the mutex while waiting
		// This allows other threads to acquire and release locks
		lt.mu.Unlock()

		// Wait either for a signal from the condition variable or a timeout
		select {
		case <-waitCh:
			// The condition variable was signaled, reacquire the mutex to check conditions again
			// lt.mu.Lock()
		case <-time.After(remainingTime):
			// Max wait time exceeded, reacquire the mutex and return an error
			lt.mu.Lock()
			return LockAbortError
		}

		lt.mu.Lock()
	}

	// Check if we still have an XLock after waiting
	if lt.hasXLock(block) {
		return LockAbortError
	}

	// Grant the shared lock by incrementing the lock count
	val := lt.getLockVal(block)
	lt.locks[block] = val + 1
	return nil
}

func (lt *LockTable) XLock(block *file.BlockID) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	startTime := time.Now()

	// Wait if there are multiple shared locks
	for lt.hasAnyLock(block) && !lt.waitingTooLong(startTime) {
		//  Wait with a timeout
		waitCh := make(chan struct{})

		go func() {
			lt.cond.L.Lock()
			lt.cond.Wait() // Atomically releases lock and blocks until signaled
			lt.cond.L.Unlock()
			close(waitCh)
		}()

		// Temporarily release the mutex while waiting
		lt.mu.Unlock()

		// Wait for signal or timeout
		select {
		case <-waitCh:
			lt.mu.Lock()
		case <-time.After(MaxWaitTime - time.Since(startTime)):
			// TImeout occured
			lt.mu.Lock()
			return LockAbortError
		}
	}

	// Check if we still have other locks after waiting
	if lt.hasOtherSLocks(block) {
		return LockAbortError
	}

	lt.locks[block] = -1
	return nil
}

// Releases a lock on the specified block and notifies waiting goroutines if this was the last lock on the block
func (lt *LockTable) Unlock(block *file.BlockID) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	val := lt.getLockVal(block)

	if val > 1 {
		// Decrement the shared lock count
		lt.locks[block] = val - 1
	} else if val != 0 {
		// Remove the lock entry entirely
		delete(lt.locks, block)
		// Notify all waiting goroutines
		lt.cond.Broadcast()
	}
}

// Checks if the block has an exclusive lock
func (lt *LockTable) hasXLock(block *file.BlockID) bool {
	return lt.getLockVal(block) < 0
}

// Checks if the block has multiple shared locks
func (lt *LockTable) hasOtherSLocks(block *file.BlockID) bool {
	return lt.getLockVal(block) > 1
}

func (lt *LockTable) hasAnyLock(block *file.BlockID) bool {
	val, exists := lt.locks[block]
	return exists && val != 0
}

func (lt *LockTable) waitingTooLong(startTime time.Time) bool {
	return time.Since(startTime) > MaxWaitTime
}

func (lt *LockTable) getLockVal(block *file.BlockID) int {
	val, exists := lt.locks[block]

	if !exists {
		return 0
	}

	return val
}

// Testing methods
func (lt *LockTable) GetLockVal(block *file.BlockID) int {
	val, exists := lt.locks[block]

	if !exists {
		return 0
	}

	return val
}

func (lt *LockTable) GetLocks() map[*file.BlockID]int {
	return lt.locks
}
