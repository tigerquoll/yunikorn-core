//go:build deadlock

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

package locking

import (
	"sync"
	"testing"

	"gotest.tools/v3/assert"
)

// resetClassCheck puts the checker back into a known state: no goroutine holds anything, no pair
// has been reported and no deadlock has been flagged.
func resetClassCheck(t *testing.T, enabled bool) {
	t.Helper()
	for i := range heldShards {
		heldShards[i].lock.Lock()
		heldShards[i].goroutines = make(map[int64]*held)
		heldShards[i].lock.Unlock()
	}
	for i := Class(0); i < numClasses; i++ {
		for j := Class(0); j < numClasses; j++ {
			reported[i][j].Store(false)
		}
	}
	deadlockDetected.Store(false)
	reportCount.Store(0)
	testingMode.Store(true)
	classOrderEnabled.Store(enabled)
	t.Cleanup(func() {
		classOrderEnabled.Store(false)
		testingMode.Store(false)
		deadlockDetected.Store(false)
	})
}

// tripped reports whether the given ordered pair has been reported by the checker. The tests use
// this rather than IsDeadlockDetected: the detected flag is shared with the go-deadlock watchdog,
// which fires asynchronously from other tests in this same binary.
func tripped(heldClass, acquired Class) bool {
	return reported[heldClass][acquired].Load()
}

// anyTripped reports whether the checker reported any pair at all.
func anyTripped() bool {
	for i := Class(0); i < numClasses; i++ {
		for j := Class(0); j < numClasses; j++ {
			if reported[i][j].Load() {
				return true
			}
		}
	}
	return false
}

func classed(c Class) *RWMutex {
	m := &RWMutex{}
	m.SetClass(c)
	return m
}

// TestClassOrderUpwardIsViolation takes a lock lower in the order and then one above it.
func TestClassOrderUpwardIsViolation(t *testing.T) {
	resetClassCheck(t, true)
	app := classed(ClassApplication)
	partition := classed(ClassPartitionContext)

	app.Lock()
	assert.Assert(t, !tripped(ClassApplication, ClassPartitionContext), "holding the application lock alone is not a violation")
	partition.Lock()
	assert.Assert(t, tripped(ClassApplication, ClassPartitionContext), "taking the partition lock under the application lock must be reported")
	partition.Unlock()
	app.Unlock()
}

// TestClassOrderDownwardIsAllowed walks the documented order from the top down.
func TestClassOrderDownwardIsAllowed(t *testing.T) {
	resetClassCheck(t, true)
	cluster := classed(ClassClusterContext)
	partition := classed(ClassPartitionContext)
	app := classed(ClassApplication)
	node := classed(ClassNode)

	cluster.Lock()
	partition.Lock()
	app.Lock()
	node.RLock()
	assert.Assert(t, !anyTripped(), "the documented order must not be reported")
	node.RUnlock()
	app.Unlock()
	partition.Unlock()
	cluster.Unlock()
}

// TestClassOrderTransitive checks the closure: the direct edges do not relate a user tracker to the
// cluster context but the closure does.
func TestClassOrderTransitive(t *testing.T) {
	resetClassCheck(t, true)
	tracker := classed(ClassUserTracker)
	cluster := classed(ClassClusterContext)

	tracker.Lock()
	cluster.Lock()
	assert.Assert(t, tripped(ClassUserTracker, ClassClusterContext), "cluster context under a user tracker is an upward acquisition")
	cluster.Unlock()
	tracker.Unlock()
}

// TestClassOrderUnrelatedPairSilent uses two classes the document does not order against each
// other. Guessing an order there would only produce noise.
func TestClassOrderUnrelatedPairSilent(t *testing.T) {
	resetClassCheck(t, true)
	node := classed(ClassNode)
	queue := classed(ClassQueue)

	node.Lock()
	queue.Lock()
	assert.Assert(t, !anyTripped(), "node and queue are not ordered against each other")
	queue.Unlock()
	node.Unlock()

	// and the other way round
	resetClassCheck(t, true)
	queue.Lock()
	node.Lock()
	assert.Assert(t, !anyTripped(), "node and queue are not ordered against each other")
	node.Unlock()
	queue.Unlock()
}

// TestClassOrderClassless leaves the locks without a class: they must not be tracked at all.
func TestClassOrderClassless(t *testing.T) {
	resetClassCheck(t, true)
	one := &RWMutex{}
	two := &RWMutex{}

	one.Lock()
	two.Lock()
	assert.Assert(t, !anyTripped(), "locks without a class are not ordered")
	two.Unlock()
	one.Unlock()
}

// TestClassOrderSameClass nests two locks of the same class. That is a violation unless the class
// is a hierarchy or its same class rule is withheld.
func TestClassOrderSameClass(t *testing.T) {
	resetClassCheck(t, true)
	first := classed(ClassNode)
	second := classed(ClassNode)

	first.Lock()
	second.Lock()
	assert.Assert(t, tripped(ClassNode, ClassNode), "nesting two node locks must be reported")
	second.Unlock()
	first.Unlock()
}

// TestClassOrderSameClassWithheld covers the classes whose same class rule is not enforced yet.
func TestClassOrderSameClassWithheld(t *testing.T) {
	resetClassCheck(t, true)
	first := classed(ClassApplication)
	second := classed(ClassApplication)

	first.Lock()
	second.Lock()
	assert.Assert(t, !anyTripped(), "the application same class rule is withheld in this version")
	second.Unlock()
	first.Unlock()
}

// TestClassOrderHierarchyExempt nests two queue locks, the queue hierarchy is exempt.
func TestClassOrderHierarchyExempt(t *testing.T) {
	resetClassCheck(t, true)
	parent := classed(ClassQueue)
	child := classed(ClassQueue)

	parent.Lock()
	child.Lock()
	assert.Assert(t, !anyTripped(), "the queue hierarchy is exempt from the same class rule")
	child.Unlock()
	parent.Unlock()
}

// TestClassOrderDisabled repeats the violation case with the checker turned off.
func TestClassOrderDisabled(t *testing.T) {
	resetClassCheck(t, false)
	app := classed(ClassApplication)
	partition := classed(ClassPartitionContext)

	app.Lock()
	partition.Lock()
	assert.Assert(t, !anyTripped(), "nothing is checked while the switch is off")
	partition.Unlock()
	app.Unlock()

	// and nothing was tracked either
	total := 0
	for i := range heldShards {
		heldShards[i].lock.Lock()
		total += len(heldShards[i].goroutines)
		heldShards[i].lock.Unlock()
	}
	assert.Equal(t, 0, total, "no goroutine state may be kept while the switch is off")
}

// TestClassOrderPerGoroutine holds a low lock on one goroutine while another goroutine takes a
// high one: the two must not see each other's held set.
func TestClassOrderPerGoroutine(t *testing.T) {
	resetClassCheck(t, true)
	app := classed(ClassApplication)
	partition := classed(ClassPartitionContext)

	held := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		app.Lock()
		close(held)
		<-release
		app.Unlock()
	}()

	<-held
	// this goroutine holds nothing, so the partition lock is fine even though the other goroutine
	// is sitting on an application lock
	partition.Lock()
	assert.Assert(t, !anyTripped(), "held classes must not leak between goroutines")
	partition.Unlock()
	close(release)
	<-done
}

// TestClassOrderUnlockOutOfOrder releases the locks in acquisition order rather than in reverse:
// the multiset must still end up empty.
func TestClassOrderUnlockOutOfOrder(t *testing.T) {
	resetClassCheck(t, true)
	cluster := classed(ClassClusterContext)
	partition := classed(ClassPartitionContext)
	app := classed(ClassApplication)

	cluster.Lock()
	partition.Lock()
	app.Lock()
	// release in the same order they were taken
	cluster.Unlock()
	partition.Unlock()
	app.Unlock()
	assert.Assert(t, !anyTripped(), "the documented order must not be reported")

	total := 0
	for i := range heldShards {
		heldShards[i].lock.Lock()
		total += len(heldShards[i].goroutines)
		heldShards[i].lock.Unlock()
	}
	assert.Equal(t, 0, total, "the goroutine must be dropped once it holds nothing")
}

// TestClassOrderReportOnce trips the same pair twice and checks only one report is produced.
func TestClassOrderReportOnce(t *testing.T) {
	resetClassCheck(t, true)
	app := classed(ClassApplication)
	partition := classed(ClassPartitionContext)

	app.Lock()
	partition.Lock()
	partition.Unlock()
	app.Unlock()
	assert.Assert(t, tripped(ClassApplication, ClassPartitionContext), "the pair is marked as reported")

	reportCount.Store(0)
	app.Lock()
	partition.Lock()
	assert.Equal(t, int32(0), reportCount.Load(), "the same pair must only be reported once")
	partition.Unlock()
	app.Unlock()
}

// TestClassOrderReadLocksOrder checks the read side is ordered the same way as the write side.
func TestClassOrderReadLocksOrder(t *testing.T) {
	resetClassCheck(t, true)
	tracker := classed(ClassGroupTracker)
	manager := classed(ClassUGMManager)

	tracker.RLock()
	manager.RLock()
	assert.Assert(t, tripped(ClassGroupTracker, ClassUGMManager), "the manager read lock under a tracker read lock is upward")
	manager.RUnlock()
	tracker.RUnlock()
}

// TestClassOrderConcurrentNoRace hammers the checker from several goroutines, it is here to be run
// under the race detector.
func TestClassOrderConcurrentNoRace(t *testing.T) {
	resetClassCheck(t, true)
	manager := classed(ClassUGMManager)
	tracker := classed(ClassUserTracker)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				manager.Lock()
				tracker.Lock()
				tracker.Unlock()
				manager.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.Assert(t, !anyTripped(), "the documented order must not be reported")
}
