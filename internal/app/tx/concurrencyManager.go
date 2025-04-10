package tx

import (
	"centauri/internal/app/file"
	"sync"
)

const shared string = "S"    // represents a shared (read) lock
const exclusive string = "X" // represents an exclusive (write) lock

// Handles transaction-level concurrency control.
// Each transaction has its own concurrencyManager instance that tracks
// which locks the transaction currently holds and coordinates with
// the global lock table for lock acquistion and release.
type ConcurrencyManager struct {
	locks     map[file.BlockID]string // Tracks the types of locks this transaction holds on each block
	locktable *LockTable              // Global lock manager shared by all transactions, using pointer ensures all transactions refer to the same instance
	mu        sync.RWMutex            // protects concurrent access to the locks map
}

func NewConcurrencyManager(lt *LockTable) *ConcurrencyManager {
	return &ConcurrencyManager{
		locks:     make(map[file.BlockID]string),
		locktable: lt,
	}
}

// Obtains a shared lock on the specified block.
// If the transaction does`nt already have any lock on the block,
// it requests one from the global lock table and records it locally.
func (cm *ConcurrencyManager) SLock(block file.BlockID) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check if we already have any lock on this block
	if _, exists := cm.locks[block]; !exists {
		// Request shared lock from global lock table
		if err := cm.locktable.SLock(&block); err != nil {
			return err
		}
		// Record the lock in our local map
		cm.locks[block] = shared
	}

	return nil
}

// Obtains an exclusive lock on the specified block.
// If the transaction does`nt have an exclusive lock already:
// 1. First obtains a shared lock (if necessary)
// 2. Then upgrades it to an exclusive lock
// This two-step process helps prevent deadlocks
func (cm *ConcurrencyManager) XLock(block file.BlockID) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.hasXLock(block) {
		// First get a shared lock if we dont have any
		if _, exists := cm.locks[block]; !exists {
			if err := cm.locktable.SLock(&block); err != nil {
				return err
			}
			cm.locks[block] = shared
		}

		// Now upgrade to exclusive lock
		if err := cm.locktable.XLock(&block); err != nil {
			return err
		}

		cm.locks[block] = exclusive
	}

	return nil
}

// Releases all locks helf by this transaction.
// It should be called when the transaction commits or rolls back.
func (cm *ConcurrencyManager) Release() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Release each lock in the global lock table
	for block := range cm.locks {
		cm.locktable.Unlock(&block)
	}

	// Clear out our local lock tracking
	clear(cm.locks)
}

// Checks if the transaction currently holds an
// exclusive lock on the specified lock.
func (cm *ConcurrencyManager) hasXLock(block file.BlockID) bool {
	lockType, exists := cm.locks[block]
	return exists && lockType == exclusive
}
