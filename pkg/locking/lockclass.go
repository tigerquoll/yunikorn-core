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

// The taxonomy below is the same order the runtime checker in this package enforces, in the
// form the static analyzer reads. The two are deliberately side by side so they cannot drift,
// and TestDeclaredOrderMatchesAnnotations fails when they do.
//
// +lockorder:ClusterContext < PartitionContext
// +lockorder:PartitionContext < Application
// +lockorder:Application < Queue
// +lockorder:Application < Node
// +lockorder:Application < UGMManager
// +lockorder:UGMManager < UserTracker
// +lockorder:UGMManager < GroupTracker
// +lockhierarchical:Queue
// +lockorderwithheld:Application
package locking

import (
	"sync/atomic"
)

// Class is the ordering class of a lock. Locks that are not given a class are not part of the
// order check at all: they are still covered by the go-deadlock detection.
//
// The classes and the order are the scheduler lock order as documented in
// docs/how-yunikorn-works.md section 4.1 "The lock order". Only the classes that the document
// actually orders are modelled, anything else stays classless in this version. The order itself
// and the checking live in lockclass_deadlock.go: the check is only compiled into the deadlock
// tagged build so that the default build keeps the plain, inlinable forwarders, see forwarders.go.
type Class uint32

const (
	// ClassNone is the zero value: the lock is not part of the order check.
	ClassNone Class = iota
	ClassClusterContext
	ClassPartitionContext
	ClassApplication
	ClassQueue
	ClassNode
	ClassUGMManager
	ClassUserTracker
	ClassGroupTracker
	numClasses
)

var className = [numClasses]string{
	ClassNone:             "none",
	ClassClusterContext:   "ClusterContext",
	ClassPartitionContext: "PartitionContext",
	ClassApplication:      "Application",
	ClassQueue:            "Queue",
	ClassNode:             "Node",
	ClassUGMManager:       "ugm.Manager",
	ClassUserTracker:      "UserTracker",
	ClassGroupTracker:     "GroupTracker",
}

func (c Class) String() string {
	if c >= numClasses {
		return "unknown"
	}
	return className[c]
}

// classOrderEnabled is the master switch, set from DEADLOCK_CLASS_ORDER_ENABLED. It is read on
// every acquisition in the deadlock tagged build so it must stay a plain atomic load.
var classOrderEnabled atomic.Bool

// IsClassOrderEnabled returns true when the lock class order check is turned on. It is always
// false in a build that does not carry the check.
func IsClassOrderEnabled() bool {
	return classOrderEnabled.Load()
}

// SetClass gives the lock an ordering class. It is called once from the constructor of the object
// the lock belongs to, before the object is shared, so it does not need to be ordered against the
// acquisitions of that lock. It is a plain store in every build, the class is only read by the
// check in the deadlock tagged build.
func (m *Mutex) SetClass(c Class) {
	m.class.Store(uint32(c))
}

// SetClass gives the lock an ordering class, see Mutex.SetClass.
func (m *RWMutex) SetClass(c Class) {
	m.class.Store(uint32(c))
}
