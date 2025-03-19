package tx

import (
	"centauri/internal/app/buffer"
	"centauri/internal/app/file"
	"fmt"
	"sync"
)

// Manages a collections of buffers for a transaction.
// It tracks pinned buffers and handles buffer pinning/unpinning operations
type BufferList struct {
	buffers map[file.BlockID]*buffer.Buffer
	pins    []file.BlockID
	bm      *buffer.BufferManager
	mu      sync.RWMutex
}

func NewBufferList(bm *buffer.BufferManager) *BufferList {
	return &BufferList{
		buffers: make(map[file.BlockID]*buffer.Buffer),
		pins:    make([]file.BlockID, 0),
		bm:      bm,
	}
}

// Retrieves a buffer for the specified block
func (bl *BufferList) GetBuffer(block file.BlockID) (*buffer.Buffer, error) {
	bl.mu.RLock()
	defer bl.mu.RUnlock()

	// Return the buffer if it exists in our map
	if buff, exists := bl.buffers[block]; exists {
		return buff, nil
	}

	return nil, fmt.Errorf("buffer not found for block: %v", block)
}

// Associates a buffer with a block and marks it as pinned
func (bl *BufferList) Pin(block file.BlockID) error {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	// Pin the buffer using the buffer manager
	buff, err := bl.bm.Pin(&block)
	if err != nil {
		return fmt.Errorf("failed to pin buffer: %w", err)
	}

	// Add to our tracked buffer and pins
	bl.buffers[block] = buff
	bl.pins = append(bl.pins, block)
	return nil
}

// Removes the pin from a block and potentially releases its buffer
func (bl *BufferList) Unpin(block file.BlockID) error {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	// Get the buffer for this block
	buff, exists := bl.buffers[block]
	if !exists {
		return fmt.Errorf("no buffer found for block: %v", block)
	}

	// Unpin in the buffer manager
	bl.bm.Unpin(buff)

	// Remove from pins slice efficiently
	for i, pinnedBlock := range bl.pins {
		if pinnedBlock == block {
			// Remove without preserving order for better performance
			bl.pins[i] = bl.pins[len(bl.pins)-1]
			bl.pins = bl.pins[:len(bl.pins)-1]
			break
		}
	}

	// If block is no longer pinned anywhere, remove from buffers
	stillPinned := false
	for _, pinnedBlock := range bl.pins {
		if pinnedBlock == block {
			stillPinned = true
			break
		}
	}

	if !stillPinned {
		delete(bl.buffers, block)
	}

	return nil
}

// Releases all pinned buffers
func (bl *BufferList) UnpinAll() error {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	// Unpin each buffer in the buffer manager
	for _, block := range bl.pins {
		if buff, exists := bl.buffers[block]; exists {
			bl.bm.Unpin(buff)
		}
	}

	// Clear all tracking maps and slices
	bl.buffers = make(map[file.BlockID]*buffer.Buffer)
	bl.pins = bl.pins[:0] // Preserve capacity while clearing

	return nil
}
