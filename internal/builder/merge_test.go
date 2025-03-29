// pthash-go/internal/builder/merge_test.go
package builder

import (
	"pthashgo/internal/core"
	"reflect"
	"sort"
	"testing"
)

// Remove mockMerger struct and its helper

func TestMergeSingleBlock(t *testing.T) {
	pairs := pairsT{
		{BucketID: 1, Payload: 101},
		{BucketID: 1, Payload: 102},
		{BucketID: 3, Payload: 301},
		{BucketID: 5, Payload: 501},
		{BucketID: 5, Payload: 502},
		{BucketID: 5, Payload: 503},
	}
	merger := newBucketsT() // Use the actual bucketsT
	err := mergeSingleBlock(pairs, merger, false, false)
	if err != nil {
		t.Fatalf("mergeSingleBlock failed: %v", err)
	}

	if merger.numBuckets() != 3 {
		t.Errorf("Expected 3 buckets, got %d", merger.numBuckets())
	}
	if merger.maxBucketSize != 3 {
		t.Errorf("Expected maxSize 3, got %d", merger.maxBucketSize)
	}

	// Verify content using iterator
	expectedBuckets := map[core.BucketIDType][]uint64{
		1: {101, 102},
		3: {301},
		5: {501, 502, 503},
	}
	foundBuckets := make(map[core.BucketIDType][]uint64)
	iter := merger.iterator()
	for iter.HasNext() {
		bucket := iter.Next()
		// Copy payloads as the underlying buffer might change if merger was reused (it's not here)
		pCopy := make([]uint64, len(bucket.Payloads()))
		copy(pCopy, bucket.Payloads())
		foundBuckets[bucket.ID()] = pCopy
	}

	if !reflect.DeepEqual(foundBuckets, expectedBuckets) {
		t.Errorf("Merged buckets mismatch:\ngot:  %v\nwant: %v", foundBuckets, expectedBuckets)
	}
}

// TestMergeSingleBlockDuplicatePayload remains the same, checking the error

func TestMergeMultipleBlocks(t *testing.T) {
	block1 := pairsT{
		{BucketID: 1, Payload: 101}, {BucketID: 3, Payload: 301},
		{BucketID: 5, Payload: 501}, {BucketID: 5, Payload: 503},
	}
	block2 := pairsT{
		{BucketID: 1, Payload: 102}, {BucketID: 2, Payload: 201},
		{BucketID: 5, Payload: 502}, {BucketID: 6, Payload: 601},
	}
	block3 := pairsT{
		{BucketID: 3, Payload: 302}, {BucketID: 5, Payload: 500},
	}
	// Sort blocks to ensure preconditions (ASC ID, ASC Payload)
	sort.Slice(block1, func(i, j int) bool { return block1[i].Less(block1[j]) })
	sort.Slice(block2, func(i, j int) bool { return block2[i].Less(block2[j]) })
	sort.Slice(block3, func(i, j int) bool { return block3[i].Less(block3[j]) })

	blocks := []pairsT{block1, block2, block3}
	merger := newBucketsT() // Use actual bucketsT
	// Assuming ASC sort for merge (secondarySort = false)
	err := mergeMultipleBlocks(blocks, merger, false, false)
	if err != nil {
		t.Fatalf("mergeMultipleBlocks failed: %v", err)
	}

	expectedBuckets := map[core.BucketIDType][]uint64{
		1: {101, 102},
		2: {201},
		3: {301, 302},
		5: {500, 501, 502, 503},
		6: {601},
	}

	if merger.numBuckets() != uint64(len(expectedBuckets)) {
		t.Errorf("Expected %d buckets, got %d", len(expectedBuckets), merger.numBuckets())
	}
	if merger.maxBucketSize != 4 {
		t.Errorf("Expected maxSize 4, got %d", merger.maxBucketSize)
	}

	// Verify content using iterator
	foundBuckets := make(map[core.BucketIDType][]uint64)
	iter := merger.iterator()
	count := 0
	for iter.HasNext() {
		count++
		bucket := iter.Next()
		pCopy := make([]uint64, len(bucket.Payloads()))
		copy(pCopy, bucket.Payloads())
		foundBuckets[bucket.ID()] = pCopy
	}
	if uint64(count) != merger.numBuckets() {
		t.Errorf("Iterator iterated %d times, but merger reports %d buckets", count, merger.numBuckets())
	}

	if !reflect.DeepEqual(foundBuckets, expectedBuckets) {
		t.Errorf("Merged buckets mismatch:\ngot:  %v\nwant: %v", foundBuckets, expectedBuckets)
	}
}

func TestBucketsIterator(t *testing.T) {
	// Create a bucketsT structure manually
	merger := newBucketsT()
	// Buckets of size 2: ID 10, ID 30
	merger.add(10, []uint64{101, 102})
	merger.add(30, []uint64{301, 302})
	// Buckets of size 1: ID 20, ID 5
	merger.add(20, []uint64{201})
	merger.add(5, []uint64{51})
	// Buckets of size 3: ID 15
	merger.add(15, []uint64{151, 152, 153})
	// No buckets of size 4+

	if merger.maxBucketSize != 3 {
		t.Fatalf("Expected max bucket size 3, got %d", merger.maxBucketSize)
	}
	if merger.numBuckets() != 5 {
		t.Fatalf("Expected 5 total buckets, got %d", merger.numBuckets())
	}

	iter := merger.iterator()
	var iteratedBuckets []core.BucketT

	// Iterate and collect
	for iter.HasNext() {
		bucket := iter.Next()
		// Copy data for verification, as underlying slice might be reused? (No, buffers not reused here)
		dataCopy := make([]uint64, len(bucket.Data()))
		copy(dataCopy, bucket.Data())
		iteratedBuckets = append(iteratedBuckets, core.NewBucketT(dataCopy, bucket.Size()))
	}

	// Expected order (descending size): size 3, size 2, size 1
	// Within a size, order is not strictly guaranteed by this test setup,
	// but it should match the order they were added *within that size buffer*.
	expectedOrder := []struct {
		ID   core.BucketIDType
		Size core.BucketSizeType
		Data []uint64 // Includes ID
	}{
		{15, 3, []uint64{15, 151, 152, 153}}, // Size 3 first
		{10, 2, []uint64{10, 101, 102}},      // Size 2 next
		{30, 2, []uint64{30, 301, 302}},
		{20, 1, []uint64{20, 201}}, // Size 1 last
		{5, 1, []uint64{5, 51}},
	}

	if len(iteratedBuckets) != len(expectedOrder) {
		t.Fatalf("Iterator produced %d buckets, expected %d", len(iteratedBuckets), len(expectedOrder))
	}

	for i, expected := range expectedOrder {
		got := iteratedBuckets[i]
		if got.ID() != expected.ID || got.Size() != expected.Size || !reflect.DeepEqual(got.Data(), expected.Data) {
			t.Errorf("Mismatch at index %d:\ngot:  ID=%d Size=%d Data=%v\nwant: ID=%d Size=%d Data=%v",
				i, got.ID(), got.Size(), got.Data(), expected.ID, expected.Size, expected.Data)
		}
	}

	// Test empty case
	emptyMerger := newBucketsT()
	emptyIter := emptyMerger.iterator()
	if emptyIter.HasNext() {
		t.Error("Iterator on empty merger should not have next")
	}

}
