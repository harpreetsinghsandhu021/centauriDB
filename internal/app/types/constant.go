package types

import (
	"fmt"
	"hash/fnv"
	"strings"

	"golang.org/x/text/unicode/norm"
)

// Represents a value that can be either an integer or a string.
// Implements comparable operations and string conversion.
type Constant struct {
	iVal *int
	sVal *string
}

func NewConstantInt(iVal int) *Constant {
	return &Constant{
		iVal: &iVal,
	}
}

func NewConstantString(sVal string) *Constant {
	return &Constant{
		sVal: &sVal,
	}
}

// Returns the integer value
func (c *Constant) AsInt() *int {
	return c.iVal
}

// Returns the string value
func (c *Constant) AsString() *string {
	return c.sVal
}

// Compares this Constant with another value.
func (c *Constant) Equals(obj interface{}) bool {
	otherConst, ok := obj.(*Constant)

	if !ok {
		return false
	}

	if c.iVal != nil && otherConst.iVal != nil {
		return *c.iVal == *otherConst.iVal
	}

	if c.sVal != nil && otherConst.sVal != nil {
		return *c.sVal == *otherConst.sVal
	}

	return false
}

// Implements comparision between Constants
func (c *Constant) CompareTo(other *Constant) int {
	if c.iVal != nil && other.iVal != nil {
		if *c.iVal < *other.iVal {
			return -1
		} else if *c.iVal > *other.iVal {
			return 1
		}
		return 0
	}

	if c.sVal != nil && other.sVal != nil {
		return strings.Compare(*c.sVal, *other.sVal)
	}

	panic("Cannot compare constants of different types")
}

// Generates a Hash code for the constant.
// The hash is computed using the FNV-1a hash algo which provides
func (c *Constant) HashCode() uint64 {
	h := fnv.New64()

	if c.iVal != nil {
		// For integer values, convert to string then to bytes
		intBytes := []byte(fmt.Sprintf("%d", *c.iVal))
		h.Write(intBytes)
	} else if c.sVal != nil {
		// For string values, normalize Unicode and convert to bytes
		normalized := norm.NFKC.String(*c.sVal)
		h.Write([]byte(normalized))
	}

	return h.Sum64()
}

// Returns a string representation of the constant
func (c *Constant) String() string {
	if c.iVal != nil {
		return fmt.Sprintf("%d", *c.iVal)
	}

	return *c.sVal
}
