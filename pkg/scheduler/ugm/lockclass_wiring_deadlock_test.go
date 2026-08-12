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

package ugm

import (
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-core/pkg/locking"
)

// TestLockClassWiringUGM checks the real ugm constructors wire their classes up.
//
// The manager is taken through GetUserManager rather than newManager: newManager does not publish
// the singleton, and the tracker constructors reach it through the package global on their way into
// newQueueTracker, so building a tracker without a published manager panics.
func TestLockClassWiringUGM(t *testing.T) {
	manager := GetUserManager()
	assert.Equal(t, locking.ClassUGMManager, manager.GetClass(), "the manager constructor must set the manager lock class")

	events := newUGMEvents(mock.NewEventSystemDisabled())
	userTracker := newUserTracker("class-user", events)
	assert.Equal(t, locking.ClassUserTracker, userTracker.GetClass(), "newUserTracker must set the user tracker lock class")

	groupTracker := newGroupTracker("class-group", events)
	assert.Equal(t, locking.ClassGroupTracker, groupTracker.GetClass(), "newGroupTracker must set the group tracker lock class")
}

// TestLockClassEdgeManagerUserTracker drives the ugm.Manager before UserTracker edge on real
// objects: the tracker lock is held and the manager lock is taken, which is upward.
func TestLockClassEdgeManagerUserTracker(t *testing.T) {
	locking.ResetClassOrderCheck(true)
	t.Cleanup(func() { locking.ResetClassOrderCheck(false) })

	manager := GetUserManager()
	userTracker := newUserTracker("class-user", newUGMEvents(mock.NewEventSystemDisabled()))

	userTracker.RLock()
	manager.RLock()
	manager.RUnlock()
	userTracker.RUnlock()
	assert.Assert(t, locking.ClassOrderTripped(locking.ClassUserTracker, locking.ClassUGMManager),
		"taking the manager lock while holding a user tracker lock must be reported")
}

// TestLockClassEdgeManagerGroupTracker drives the ugm.Manager before GroupTracker edge.
func TestLockClassEdgeManagerGroupTracker(t *testing.T) {
	locking.ResetClassOrderCheck(true)
	t.Cleanup(func() { locking.ResetClassOrderCheck(false) })

	manager := GetUserManager()
	groupTracker := newGroupTracker("class-group", newUGMEvents(mock.NewEventSystemDisabled()))

	groupTracker.RLock()
	manager.RLock()
	manager.RUnlock()
	groupTracker.RUnlock()
	assert.Assert(t, locking.ClassOrderTripped(locking.ClassGroupTracker, locking.ClassUGMManager),
		"taking the manager lock while holding a group tracker lock must be reported")
}
