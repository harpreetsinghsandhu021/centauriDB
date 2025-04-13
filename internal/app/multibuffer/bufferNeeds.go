package multibuffer

import "math"

// Provides utility functions for estimating the optimal number of buffers to allocate for a scan
type BufferNeeds struct {
	// This is empty struct because all methods are static
}

// Considers the various roots of the specified output size (in blocks),
// and returns the highest root that is less than the number of available buffers.
// We reserve a couple of buffers so that we don't run completely out.
// In simple words,It Calculates the optimal root value for buffer allocation given available buffers and size
// Parameters:
//
//	available: total number of buffers available
//	size: total number of blocks to process
//
// Returns: the highest possible root value that's less than available buffers
func BestRoot(available, size int) int {
	// Reserve 2 buffers for safety, leaving 'avail' buffers for actual use
	avail := available - 2

	// If we have 1 or fewer buffers available after reserve, return 1
	if avail <= 1 {
		return 1
	}

	// Initialize k to maximum possible integer value
	k := math.MaxInt32
	// i represents the root degree we're trying (starts at 1.0)
	i := 1.0

	// Keep trying higher roots until we find one that fits within available buffers
	for k > avail {
		// Increment the root degree
		i++
		// Calculate k = size^(1/i), rounded up
		// This represents the number of buffers needed for an i-th root scheme
		k = int(math.Ceil(math.Pow(float64(size), 1/i)))
	}

	// Return the first root value that fits within our buffer constraint
	return k
}

// Considers the various factors of the specified output size (in blocks),
// and returns the highest factor that is less than the number of available buffers.
// We reserve a couple of buffers so that we don't run completely out.
func BestFactor(available, size int) int {
	avail := available - 2

	if avail <= 1 {
		return 1
	}

	k := size
	i := 1.0
	for k > avail {
		i++
		k = int(math.Ceil(float64(size) / i))
	}

	return k
}
