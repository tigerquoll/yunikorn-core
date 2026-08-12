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

// The helpers below exist for the tests of the packages that own the classed objects: those tests
// check that the constructors wire the classes up and that a deliberate violation on real objects
// is reported, and they cannot reach the internals of this package. Everything here is behind the
// deadlock build tag, so none of it exists in a normal build.

// GetClass returns the ordering class of the lock.
func (m *Mutex) GetClass() Class {
	return Class(m.class.Load())
}

// GetClass returns the ordering class of the lock.
func (m *RWMutex) GetClass() Class {
	return Class(m.class.Load())
}

// ResetClassOrderCheck clears everything the class order check has recorded and turns it on or off.
// It also turns on testing mode so that a deliberate violation reports instead of terminating the
// process. It assumes no other goroutine is holding a classed lock, which is true for a test that
// drives the check on its own objects.
func ResetClassOrderCheck(enabled bool) {
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
	reportCount.Store(0)
	deadlockDetected.Store(false)
	testingMode.Store(true)
	classOrderEnabled.Store(enabled)
}

// ClassOrderTripped reports whether the given ordered pair has been reported since the last reset.
// The tests use this rather than IsDeadlockDetected because that flag is shared with the
// go-deadlock watchdog, which can fire from an unrelated test in the same binary.
func ClassOrderTripped(heldClass, acquired Class) bool {
	return reported[heldClass][acquired].Load()
}

// ClassOrderTrippedAny reports whether any pair has been reported since the last reset.
func ClassOrderTrippedAny() bool {
	for i := Class(0); i < numClasses; i++ {
		for j := Class(0); j < numClasses; j++ {
			if reported[i][j].Load() {
				return true
			}
		}
	}
	return false
}
