package record

import "fmt"

type RID struct {
	blockNum int
	slot     int
}

func NewRID(blocknum int, slot int) *RID {
	return &RID{
		blockNum: blocknum,
		slot:     slot,
	}
}

func (rid *RID) BlockNumber() int {
	return rid.blockNum
}

func (rid *RID) Slot() int {
	return rid.slot
}

// Compares this RID with another object for equality
func (rid *RID) Equals(object interface{}) bool {
	// Type assert the interface to a *RID
	// This handles both type checking and conversion
	other, ok := object.(*RID)

	if !ok {
		return false // Return false if object is not an RID
	}

	// Compare both block number and slot
	return rid.blockNum == other.blockNum && rid.slot == other.slot

}

func (rid *RID) toString() string {
	return fmt.Sprintf("[%s, %d]", rid.blockNum, rid.slot)
}
