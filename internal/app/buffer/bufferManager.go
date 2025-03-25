package buffer

import (
	"centauri/internal/app/file"
	"centauri/internal/app/log"
	"sync"
	"time"
)

type BufferAbortError struct {
	message string
}

func (e BufferAbortError) Error() string {
	return e.message
}

func NewBufferAbortError(message string) BufferAbortError {
	return BufferAbortError{message: message}
}

// Manages the pinning and unpinning of buffers to blocks
type BufferManager struct {
	bufferPool   []*Buffer
	numAvailable int
	maxWaitTime  time.Duration // Maximum wait time for pinning a buffer
	mu           sync.Mutex
}

func NewBufferManager(fm *file.FileManager, lm *log.LogManager, numBuffs int) *BufferManager {
	bm := &BufferManager{
		bufferPool:   make([]*Buffer, numBuffs),
		numAvailable: numBuffs,
		maxWaitTime:  10 * time.Second,
	}

	// Intialize buffer pool
	for i := 0; i < numBuffs; i++ {
		bm.bufferPool[i] = NewBuffer(fm, lm)
	}

	return bm
}

// Returns the number of available(i.e, unpinned buffers)
func (bm *BufferManager) Available() int {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	return bm.numAvailable
}

// Flushes the dirty buffers modified by the specified transaction
func (bm *BufferManager) FlushAll(txNum int) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, buffer := range bm.bufferPool {
		if buffer.ModifyingTx() == txNum {
			buffer.Flush()
		}
	}
}

// unpins the specified data buffer
// If it`s pin count goes to zero, then notify any waiting threads
func (bm *BufferManager) Unpin(buff *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	buff.Unpin()
	if !buff.IsPinned() {
		bm.numAvailable++
		bm.mu.Unlock() // Unlock before broadcast to avoid deadlock
		bm.mu.Lock()   // Lock again after broadcast
	}
}

// Pins a buffer to the specified block, potentially waiting until buffer becomes
// available. If no buffer becomes available within a fixed time period, a BufferAbortError is thrown
func (bm *BufferManager) Pin(block *file.BlockID) (*Buffer, error) {
	bm.mu.Lock()

	defer bm.mu.Unlock()

	startTime := time.Now()
	buff, err := bm.tryToPin(block)
	if err != nil {
		return nil, err
	}

	// Wait until a buffer becomes available or timeout occurs
	for buff == nil && !bm.waitingTooLong(startTime) {
		// Release lock while waiting
		waitCh := make(chan struct{})

		go func() {
			time.Sleep(100 * time.Millisecond) // Small wait time to check periodically
			close(waitCh)
		}()

		bm.mu.Unlock()
		<-waitCh
		bm.mu.Lock()

		buff, err = bm.tryToPin(block)

		if err != nil {
			return nil, err
		}
	}

	if buff == nil {
		return nil, NewBufferAbortError("timed out waiting for buffer")
	}

	return buff, nil
}

// Checks if we`ve been waiting too long for a buffer
func (bm *BufferManager) waitingTooLong(startTime time.Time) bool {
	return time.Since(startTime) > bm.maxWaitTime
}

// Tries to pin a buffer to the specified block
// If there is already a buffer assigned to that block then buffer is used,
// otherwise, an unpinned buffer from the pool is chosen
func (bm *BufferManager) tryToPin(block *file.BlockID) (*Buffer, error) {
	// First, check if the block is already in a buffer
	buff := bm.findExistingBuffer(block)

	if buff == nil {
		// If not, choose an unpinned buffer
		buff = bm.chooseUnpinnedBuffer()
		if buff == nil {
			return nil, nil // No available buffers
		}

		// Assign the buffer to the block
		buff.AssignToBlock(block)
	}

	// Update available buffers count if this was unpinned
	if !buff.IsPinned() {
		bm.numAvailable--
	}

	buff.Pin()
	return buff, nil
}

// Looks for a buffer that is already assigned for the specified block
func (bm *BufferManager) findExistingBuffer(block *file.BlockID) *Buffer {
	for _, buff := range bm.bufferPool {
		b := buff.Block()
		if b != nil && b.Equals(block) {
			return buff
		}
	}

	return nil
}

// Selects an unpinned buffer from the pool
func (bm *BufferManager) chooseUnpinnedBuffer() *Buffer {
	for _, buff := range bm.bufferPool {
		if !buff.IsPinned() {
			return buff
		}
	}

	return nil
}
