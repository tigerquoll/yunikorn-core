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
	"strings"
	"sync"
	"testing"

	godeadlock "github.com/sasha-s/go-deadlock"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
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
	reports := startClassOrderCheck(t)

	cc := newClusterContext()
	pc, err := newBasePartition()
	assert.NilError(t, err, "failed to create the partition")

	pc.RLock()
	cc.RLock()
	cc.RUnlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
	pc.RUnlock()
	assert.Assert(t, strings.Contains(reports.String(), "acquiring ClusterContext while holding PartitionContext"),
		"taking the cluster context lock while holding a partition lock must be reported, got: %s", reports.String())
}

// TestLockClassEdgePartitionApplication drives the PartitionContext before Application edge.
func TestLockClassEdgePartitionApplication(t *testing.T) {
	reports := startClassOrderCheck(t)

	pc, err := newBasePartition()
	assert.NilError(t, err, "failed to create the partition")
	app := objects.NewApplication(&si.AddApplicationRequest{
		ApplicationID: "app-class-partition",
		QueueName:     "root.default",
		PartitionName: "default",
	}, security.UserGroup{User: "testuser", Groups: []string{"testgroup"}}, nil, "rm-class")

	app.RLock()
	pc.RLock()
	pc.RUnlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
	app.RUnlock()
	assert.Assert(t, strings.Contains(reports.String(), "acquiring PartitionContext while holding Application"),
		"taking the partition lock while holding an application lock must be reported, got: %s", reports.String())
}

// TestLockClassEdgeApplicationManager drives the Application before ugm.Manager edge. The manager is
// the process wide singleton, which is the one the scheduler actually uses.
func TestLockClassEdgeApplicationManager(t *testing.T) {
	reports := startClassOrderCheck(t)

	app := objects.NewApplication(&si.AddApplicationRequest{
		ApplicationID: "app-class-manager",
		QueueName:     "root.default",
		PartitionName: "default",
	}, security.UserGroup{User: "testuser", Groups: []string{"testgroup"}}, nil, "rm-class")
	manager := ugm.GetUserManager()
	assert.Equal(t, locking.ClassUGMManager, manager.GetClass(), "the manager singleton must carry the manager lock class")

	manager.RLock()
	app.RLock()
	app.RUnlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
	manager.RUnlock()
	assert.Assert(t, strings.Contains(reports.String(), "acquiring Application while holding ugm.Manager"),
		"taking the application lock while holding the manager lock must be reported, got: %s", reports.String())
}
