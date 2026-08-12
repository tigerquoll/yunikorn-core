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
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// The tests below go through the real constructors on purpose: a class that is only set by a test
// helper would pass while the scheduler runs entirely unchecked. The test helpers in this package
// build some of these objects from a struct literal, which is exactly the case that must not be
// mistaken for the real thing.

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
	locking.ResetClassOrderCheck(true)
	t.Cleanup(func() { locking.ResetClassOrderCheck(false) })

	app := NewApplication(&si.AddApplicationRequest{
		ApplicationID: "app-class-2",
		QueueName:     "root.default",
		PartitionName: "default",
	}, getTestUserGroup(), nil, "rm-class")
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create the root queue")

	queue.RLock()
	app.RLock()
	app.RUnlock()
	queue.RUnlock()
	assert.Assert(t, locking.ClassOrderTripped(locking.ClassQueue, locking.ClassApplication),
		"taking the application lock while holding a queue lock must be reported")
}

// TestLockClassEdgeApplicationNode drives the Application before Node edge on real objects.
func TestLockClassEdgeApplicationNode(t *testing.T) {
	locking.ResetClassOrderCheck(true)
	t.Cleanup(func() { locking.ResetClassOrderCheck(false) })

	app := NewApplication(&si.AddApplicationRequest{
		ApplicationID: "app-class-3",
		QueueName:     "root.default",
		PartitionName: "default",
	}, getTestUserGroup(), nil, "rm-class")
	node := NewNode(&si.NodeInfo{NodeID: "node-class-2"})

	node.RLock()
	app.RLock()
	app.RUnlock()
	node.RUnlock()
	assert.Assert(t, locking.ClassOrderTripped(locking.ClassNode, locking.ClassApplication),
		"taking the application lock while holding a node lock must be reported")
}

// TestLockClassApplicationSameClassWithheld pins the withheld pair: nesting two real application
// locks must stay silent until the two paths that do it are fixed, see sameClassWithheld in
// pkg/locking. When that rule is turned back on this test is the one to delete.
func TestLockClassApplicationSameClassWithheld(t *testing.T) {
	locking.ResetClassOrderCheck(true)
	t.Cleanup(func() { locking.ResetClassOrderCheck(false) })

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
	second.RUnlock()
	first.RUnlock()
	assert.Assert(t, !locking.ClassOrderTrippedAny(),
		"the application same class rule is withheld, nesting two application locks must stay silent")
}
