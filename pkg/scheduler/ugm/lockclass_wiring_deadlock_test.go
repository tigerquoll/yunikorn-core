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
	"strings"
	"sync"
	"testing"

	godeadlock "github.com/sasha-s/go-deadlock"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-core/pkg/locking"
)

// classOrderReports collects the reports of the class order check. It replaces the shared
// go-deadlock report buffer, which the watchdog goroutine of an unrelated test can write to at the
// same time, so the writes are guarded.
type classOrderReports struct {
	mu   sync.Mutex
	text strings.Builder
}

func (r *classOrderReports) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.text.Write(p)
}

func (r *classOrderReports) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.text.String()
}

// startClassOrderCheck turns the check on and takes over the two go-deadlock reporting options for
// the duration of the test. The report callback of the locking package exits the process when
// DEADLOCK_EXIT is set, which is what "make test" sets, so a test that trips the check on purpose
// has to hold it. The returned buffer collects the reports.
func startClassOrderCheck(t *testing.T) *classOrderReports {
	t.Helper()
	restore := locking.EnableClassOrderForTest()
	reports := &classOrderReports{}
	realBuf := godeadlock.Opts.LogBuf
	realReport := godeadlock.Opts.OnPotentialDeadlock
	godeadlock.Opts.LogBuf = reports
	godeadlock.Opts.OnPotentialDeadlock = func() {}
	t.Cleanup(func() {
		godeadlock.Opts.LogBuf = realBuf
		godeadlock.Opts.OnPotentialDeadlock = realReport
		restore()
	})
	return reports
}

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
	reports := startClassOrderCheck(t)

	manager := GetUserManager()
	userTracker := newUserTracker("class-user", newUGMEvents(mock.NewEventSystemDisabled()))

	userTracker.RLock()
	manager.RLock()
	manager.RUnlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
	userTracker.RUnlock()
	assert.Assert(t, strings.Contains(reports.String(), "acquiring ugm.Manager while holding UserTracker"),
		"taking the manager lock while holding a user tracker lock must be reported, got: %s", reports.String())
}

// TestLockClassEdgeManagerGroupTracker drives the ugm.Manager before GroupTracker edge.
func TestLockClassEdgeManagerGroupTracker(t *testing.T) {
	reports := startClassOrderCheck(t)

	manager := GetUserManager()
	groupTracker := newGroupTracker("class-group", newUGMEvents(mock.NewEventSystemDisabled()))

	groupTracker.RLock()
	manager.RLock()
	manager.RUnlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
	groupTracker.RUnlock()
	assert.Assert(t, strings.Contains(reports.String(), "acquiring ugm.Manager while holding GroupTracker"),
		"taking the manager lock while holding a group tracker lock must be reported, got: %s", reports.String())
}
