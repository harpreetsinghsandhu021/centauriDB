package tx

import (
	"centauri/internal/app/file"
	"errors"
	"sync"
	"time"
)

var ErrLockTimeout = errors.New("lock acquisition timed out")

const maxWaitTime int64 = 10000 // 10 seconds in milliseconds

// Manages locks on blocks for concurrent transactions
type LockTable struct {
	// Locks map BlockIDs to their lock status:
	// - Negative values (-1) indicate an exclusive lock (XLock)
	// - Positive values (>0) indicate the number of shared locks (SLocks)
	// - Zero or absence indicates no locks
	locks map[file.BlockID]int
	mu    sync.Mutex // Mutex for synchronization
	cond  *sync.Cond // Condition variable for wait/notify mechanism
}

// SLock acquires a shared lock on the specified block.
// It will wait up to maxWaitTime milliseconds to acquire the lock.
// Returns error if lock cannot be acquired within the time period
func (lt *LockTable) SLock(block file.BlockID) error {
	// Acquire mutex to ensure exlusive access to lock table
	lt.mu.Lock()
	// Release mutex when function returns to prevent deadlocks
	defer lt.mu.Unlock()

	// Record start time to implement timeout mechanism
	startTime := time.Now().UnixMilli()

	// Wait while there`s an exclusive lock and we haven`nt timed out
	for lt.hasXLock(block) && !waitingTooLong(startTime) {
		// Release mutex and wait for notification of lock status change
		// When notified, mutex is automatically reacquired
		lt.cond.Wait()
	}

	// If block still has an exclusive lock after waiting, we've timed out
	if lt.hasXLock(block) {
		return ErrLockTimeout
	}

	// No exclusive lock exists, safe to acquire shared lock
	// Get current lock value and increment
	lt.locks[block] = lt.getLockVal(block) + 1
	return nil
}

// XLock acquires an exclusive lock on the specified block.
// It will wait up to maxWaitTime to acquire the lock.
func (lt *LockTable) XLock(block file.BlockID) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	startTime := time.Now().UnixMilli()

	// Loop while block has any shared locks AND we have'nt exceeded timeout
	// This allows multiple attempts to acquire lock
	for lt.hasOtherSLocks(block) && !waitingTooLong(startTime) {
		// Release mutex and wait for notification of lock status change
		// When notified, mutex is automatically reacquired
		lt.cond.Wait()
	}

	// If block still has shared locks after waiting, we've timed out
	if lt.hasOtherSLocks(block) {
		return ErrLockTimeout
	}

	// No shared locks exist, safe to acquire exclusive lock
	// Set tlock value to -1 to indicate exclusive lock
	lt.locks[block] = -1
	return nil
}

func (lt *LockTable) Unlock(block file.BlockID) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	// Get current lock value for the block
	val := lt.getLockVal(block)

	// If multiple shared locks exits
	if val > 1 {
		// Decre,emt the shared lock amount
		lt.locks[block] = val - 1
	} else {
		// Otherwise remove the lock entry entirely
		delete(lt.locks, block)
		// Notify all waiting goroutines that lock status has changed
		lt.cond.Broadcast()
	}
}

func (lt *LockTable) hasXLock(block file.BlockID) bool {
	return lt.getLockVal(block) < 0
}

// Returns the lock value for the block
func (lt *LockTable) getLockVal(block file.BlockID) int {
	val, exists := lt.locks[block]

	if !exists {
		return 0
	}

	return val
}

func waitingTooLong(startTime int64) bool {
	return time.Now().UnixMilli()-startTime > maxWaitTime
}

func (lt *LockTable) hasOtherSLocks(block file.BlockID) bool {
	return lt.getLockVal(block) > 0
}
