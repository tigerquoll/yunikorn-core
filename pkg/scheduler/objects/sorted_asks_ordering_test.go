/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package objects

import (
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

// Ordering of sortedRequests for asks that tie on (priority, createTime). Ties are the normal case
// rather than an edge case: the shim reports createTime at whole-second resolution, so every ask of
// a submitted burst carries the same createTime and is separated only by the creationSeq tiebreaker
// in Allocation.LessThan.
//
// The asks below are built through NewAllocationFromSI with an explicit CreationTime tag (see
// newFuzzAsk) rather than by filling in the struct: that pins createTime deterministically and gets
// creationSeq assigned the way production assigns it, in construction order.

// tieTime is the shared, whole-second createTime of every tied ask in this file.
const tieTime = int64(100)

func newTiedAsk(key string, priority int32, creationTime int64) *Allocation {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	return newFuzzAsk(key, "app-1", "", res, false, priority, "", creationTime)
}

// askKeys returns the allocation keys of the list, front to back, for readable order assertions.
func askKeys(sorted sortedRequests) string {
	keys := make([]string, 0, len(sorted))
	for _, ask := range sorted {
		keys = append(keys, ask.allocationKey)
	}
	return strings.Join(keys, ",")
}

// TestTiedAsksKeepArrivalOrder covers the dominant case: a burst of asks that all tie is scheduled
// in the order it arrived. Every insert takes the tail fast path here, so this order is the same as
// before the creationSeq tiebreaker was added.
func TestTiedAsksKeepArrivalOrder(t *testing.T) {
	sorted := sortedRequests{}
	sorted.insert(newTiedAsk("A", 5, tieTime))
	sorted.insert(newTiedAsk("B", 5, tieTime))
	sorted.insert(newTiedAsk("C", 5, tieTime))
	assert.Equal(t, "A,B,C", askKeys(sorted), "tied asks must be attempted in arrival order")
}

// TestTiedAskInsertedMidSliceKeepsArrivalOrder covers the case the creationSeq tiebreaker changes.
// Once an ask that sorts strictly after the tie group is present - here L, at a lower priority - the
// tail fast path no longer applies and insert falls back to sort.Search. Without a strict total
// order the search predicate is true for every tie-peer, so it stops at the FIRST of them and the
// newly arrived ask jumps in front of the whole group: master orders this B,A,L (LIFO), where the
// same asks arriving in one uninterrupted burst order A,B,C (FIFO). That placement is a function of
// what else is in the slice, not of the asks being compared, so no comparator can reproduce it. It
// is deliberately normalized to arrival order here; a future reader seeing B,A,L is looking at a
// regression, not at the old behaviour.
func TestTiedAskInsertedMidSliceKeepsArrivalOrder(t *testing.T) {
	sorted := sortedRequests{}
	sorted.insert(newTiedAsk("A", 5, tieTime))
	sorted.insert(newTiedAsk("L", 1, tieTime))
	sorted.insert(newTiedAsk("B", 5, tieTime))
	assert.Equal(t, "A,B,L", askKeys(sorted), "tied ask must sort behind its earlier tie-peer")
}

// TestTiedAskReinsertRestoresSlot pins the property creationSeq exists to provide: sortedRequests
// holds pending asks only, so an ask is removed when it is allocated and inserted again when it is
// deallocated, and it has to come back to the slot it left rather than to the edge of its tie group.
func TestTiedAskReinsertRestoresSlot(t *testing.T) {
	askA := newTiedAsk("A", 5, tieTime)
	askB := newTiedAsk("B", 5, tieTime)
	askC := newTiedAsk("C", 5, tieTime)

	sorted := sortedRequests{}
	sorted.insert(askA)
	sorted.insert(askB)
	sorted.insert(askC)
	assert.Equal(t, "A,B,C", askKeys(sorted))

	// allocate the middle ask, then deallocate it again
	sorted.remove(askB)
	assert.Equal(t, "A,C", askKeys(sorted))
	sorted.insert(askB)
	assert.Equal(t, "A,B,C", askKeys(sorted), "re-inserted ask must return to its original slot")

	// same for the ask at the front of the tie group
	sorted.remove(askA)
	assert.Equal(t, "B,C", askKeys(sorted))
	sorted.insert(askA)
	assert.Equal(t, "A,B,C", askKeys(sorted), "re-inserted ask must return to its original slot")
}

// TestRemoveMatchesOnIdentity pins that remove drops the entry that IS the passed ask. The
// tracked-ask case is existing behaviour; the untracked-twin case is the behaviour change.
func TestRemoveMatchesOnIdentity(t *testing.T) {
	askA := newTiedAsk("A", 5, tieTime)
	askB := newTiedAsk("B", 5, tieTime)
	askC := newTiedAsk("C", 5, tieTime)

	// existing behaviour: distinct keys, removing the tracked ask leaves the rest in order
	sorted := sortedRequests{}
	sorted.insert(askA)
	sorted.insert(askB)
	sorted.insert(askC)
	sorted.remove(askB)
	assert.Equal(t, "A,C", askKeys(sorted), "removing the tracked ask must leave the rest in order")
	sorted.remove(askA)
	assert.Equal(t, "C", askKeys(sorted), "removing the tracked ask must leave the rest in order")

	// an ask that shares a key with a tracked one but is not in the slice removes nothing: key
	// matching would have dropped the tracked C here, leaving the caller's own ask behind instead.
	sorted = sortedRequests{}
	sorted.insert(askA)
	sorted.insert(askC)
	sorted.remove(newTiedAsk("C", 5, tieTime))
	assert.Equal(t, "A,C", askKeys(sorted), "an ask that is not in the slice must remove nothing")
	assert.Assert(t, sorted[1] == askC, "the tracked ask must still be the entry held for its key")
}

// TestLessThanStrictTotalOrder asserts that LessThan is irreflexive and asymmetric over a set that
// contains ties on priority, on createTime and on both, and that it orders every distinct pair. The
// sorted invariant of sortedRequests holds only for a comparator with those properties: the version
// without the creationSeq tiebreaker returned true in both directions for a tie and fails here.
func TestLessThanStrictTotalOrder(t *testing.T) {
	asks := []*Allocation{
		newTiedAsk("A", 5, tieTime),
		newTiedAsk("B", 5, tieTime),
		newTiedAsk("C", 5, tieTime),
		newTiedAsk("D", 5, tieTime+1),
		newTiedAsk("E", 1, tieTime),
		newTiedAsk("F", 1, tieTime+1),
	}

	for _, ask := range asks {
		assert.Assert(t, !ask.LessThan(ask), "LessThan must be irreflexive: %s", ask.allocationKey)
	}
	for _, left := range asks {
		for _, right := range asks {
			if left == right {
				continue
			}
			// exactly one direction must hold for a distinct pair
			assert.Assert(t, left.LessThan(right) != right.LessThan(left),
				"LessThan must order %s and %s in exactly one direction",
				left.allocationKey, right.allocationKey)
		}
	}
}
