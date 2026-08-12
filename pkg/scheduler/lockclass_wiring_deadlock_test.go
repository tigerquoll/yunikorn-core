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

package scheduler

import (
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// TestLockClassWiringContexts checks the real cluster and partition constructors wire their classes
// up. newBasePartition goes through newPartitionContext, the same path the scheduler uses.
func TestLockClassWiringContexts(t *testing.T) {
	cc := newClusterContext()
	assert.Equal(t, locking.ClassClusterContext, cc.GetClass(), "newClusterContext must set the cluster context lock class")

	pc, err := newBasePartition()
	assert.NilError(t, err, "failed to create the partition")
	assert.Equal(t, locking.ClassPartitionContext, pc.GetClass(), "newPartitionContext must set the partition lock class")
}

// TestLockClassEdgeClusterPartition drives the ClusterContext before PartitionContext edge on real
// objects: the partition lock is held and the cluster context lock is taken, which is upward.
func TestLockClassEdgeClusterPartition(t *testing.T) {
	locking.ResetClassOrderCheck(true)
	t.Cleanup(func() { locking.ResetClassOrderCheck(false) })

	cc := newClusterContext()
	pc, err := newBasePartition()
	assert.NilError(t, err, "failed to create the partition")

	pc.RLock()
	cc.RLock()
	cc.RUnlock()
	pc.RUnlock()
	assert.Assert(t, locking.ClassOrderTripped(locking.ClassPartitionContext, locking.ClassClusterContext),
		"taking the cluster context lock while holding a partition lock must be reported")
}

// TestLockClassEdgePartitionApplication drives the PartitionContext before Application edge.
func TestLockClassEdgePartitionApplication(t *testing.T) {
	locking.ResetClassOrderCheck(true)
	t.Cleanup(func() { locking.ResetClassOrderCheck(false) })

	pc, err := newBasePartition()
	assert.NilError(t, err, "failed to create the partition")
	app := objects.NewApplication(&si.AddApplicationRequest{
		ApplicationID: "app-class-partition",
		QueueName:     "root.default",
		PartitionName: "default",
	}, security.UserGroup{User: "testuser", Groups: []string{"testgroup"}}, nil, "rm-class")

	app.RLock()
	pc.RLock()
	pc.RUnlock()
	app.RUnlock()
	assert.Assert(t, locking.ClassOrderTripped(locking.ClassApplication, locking.ClassPartitionContext),
		"taking the partition lock while holding an application lock must be reported")
}

// TestLockClassEdgeApplicationManager drives the Application before ugm.Manager edge. The manager is
// the process wide singleton, which is the one the scheduler actually uses.
func TestLockClassEdgeApplicationManager(t *testing.T) {
	locking.ResetClassOrderCheck(true)
	t.Cleanup(func() { locking.ResetClassOrderCheck(false) })

	app := objects.NewApplication(&si.AddApplicationRequest{
		ApplicationID: "app-class-manager",
		QueueName:     "root.default",
		PartitionName: "default",
	}, security.UserGroup{User: "testuser", Groups: []string{"testgroup"}}, nil, "rm-class")
	manager := ugm.GetUserManager()
	assert.Equal(t, locking.ClassUGMManager, manager.GetClass(), "the manager singleton must carry the manager lock class")

	manager.RLock()
	app.RLock()
	app.RUnlock()
	manager.RUnlock()
	assert.Assert(t, locking.ClassOrderTripped(locking.ClassUGMManager, locking.ClassApplication),
		"taking the application lock while holding the manager lock must be reported")
}
