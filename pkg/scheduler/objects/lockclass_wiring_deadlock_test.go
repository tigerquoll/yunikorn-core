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

package objects

import (
	"strings"
	"sync"
	"testing"

	godeadlock "github.com/sasha-s/go-deadlock"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// The tests below go through the real constructors on purpose: a class that is only set by a test
// helper would pass while the scheduler runs entirely unchecked. The test helpers in this package
// build some of these objects from a struct literal, which is exactly the case that must not be
// mistaken for the real thing.

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

// TestLockClassWiringApplication checks NewApplication wires the application class up.
func TestLockClassWiringApplication(t *testing.T) {
	app := NewApplication(&si.AddApplicationRequest{
		ApplicationID: "app-class-1",
		QueueName:     "root.default",
		PartitionName: "default",
	}, getTestUserGroup(), nil, "rm-class")
	assert.Equal(t, locking.ClassApplication, app.GetClass(), "NewApplication must set the application lock class")
}

// TestLockClassWiringQueue checks the queue constructors wire the queue class up.
func TestLockClassWiringQueue(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create the root queue")
	assert.Equal(t, locking.ClassQueue, root.GetClass(), "the queue constructor must set the queue lock class")

	child, err := createManagedQueueWithProps(root, "child", false, nil, nil)
	assert.NilError(t, err, "failed to create the child queue")
	assert.Equal(t, locking.ClassQueue, child.GetClass(), "the queue constructor must set the queue lock class")
}

// TestLockClassWiringNode checks NewNode wires the node class up.
func TestLockClassWiringNode(t *testing.T) {
	node := NewNode(&si.NodeInfo{NodeID: "node-class-1"})
	assert.Assert(t, node != nil, "failed to create the node")
	assert.Equal(t, locking.ClassNode, node.GetClass(), "NewNode must set the node lock class")
}

// TestLockClassEdgeApplicationQueue drives the Application before Queue edge on real objects: the
// queue lock is held and the application lock is taken, which is upward.
func TestLockClassEdgeApplicationQueue(t *testing.T) {
	reports := startClassOrderCheck(t)

	app := NewApplication(&si.AddApplicationRequest{
		ApplicationID: "app-class-2",
		QueueName:     "root.default",
		PartitionName: "default",
	}, getTestUserGroup(), nil, "rm-class")
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create the root queue")

	queue.RLock()
	app.RLock()
	app.RUnlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
	queue.RUnlock()
	assert.Assert(t, strings.Contains(reports.String(), "acquiring Application while holding Queue"),
		"taking the application lock while holding a queue lock must be reported, got: %s", reports.String())
}

// TestLockClassEdgeApplicationNode drives the Application before Node edge on real objects.
func TestLockClassEdgeApplicationNode(t *testing.T) {
	reports := startClassOrderCheck(t)

	app := NewApplication(&si.AddApplicationRequest{
		ApplicationID: "app-class-3",
		QueueName:     "root.default",
		PartitionName: "default",
	}, getTestUserGroup(), nil, "rm-class")
	node := NewNode(&si.NodeInfo{NodeID: "node-class-2"})

	node.RLock()
	app.RLock()
	app.RUnlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
	node.RUnlock()
	assert.Assert(t, strings.Contains(reports.String(), "acquiring Application while holding Node"),
		"taking the application lock while holding a node lock must be reported, got: %s", reports.String())
}

// TestLockClassApplicationSameClassWithheld pins the withheld pair: nesting two real application
// locks must stay silent until the two paths that do it are fixed, see sameClassWithheld in
// pkg/locking. When that rule is turned back on this test is the one to delete.
func TestLockClassApplicationSameClassWithheld(t *testing.T) {
	reports := startClassOrderCheck(t)

	first := NewApplication(&si.AddApplicationRequest{
		ApplicationID: "app-class-4",
		QueueName:     "root.default",
		PartitionName: "default",
	}, getTestUserGroup(), nil, "rm-class")
	second := NewApplication(&si.AddApplicationRequest{
		ApplicationID: "app-class-5",
		QueueName:     "root.default",
		PartitionName: "default",
	}, getTestUserGroup(), nil, "rm-class")

	first.RLock()
	second.RLock()
	second.RUnlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
	first.RUnlock()
	assert.Assert(t, !strings.Contains(reports.String(), "lock order violation"),
		"the application same class rule is withheld, nesting two application locks must stay silent, got: %s", reports.String())
}
