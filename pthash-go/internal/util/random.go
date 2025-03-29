package util

import (
	"math/rand"
	"time"
)

// RandomSeed generates a random seed for hashing.
func RandomSeed() uint64 {
	// Use current time as a seed for the random number generator
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Uint64()
}
