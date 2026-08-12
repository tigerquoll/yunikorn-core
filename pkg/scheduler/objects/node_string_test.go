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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// TestNodeStringConcurrentMutation formats a node while the node is being mutated. String is
// evaluated wherever a log entry that carries the node is encoded, which is on any goroutine and
// under no lock of the node, so it cannot read the fields that the mutators write.
//
// Run with -race. On the unfixed code this reports a data race on the fields String used to read.
func TestNodeStringConcurrentMutation(t *testing.T) {
	node := newNode("node-string", map[string]resources.Quantity{"first": 100})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// the mutators: capacity, schedulable flag and the allocations map
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			node.SetCapacity(resources.NewResourceFromMap(map[string]resources.Quantity{"first": resources.Quantity(100 + i%10)}))
			node.SetSchedulable(i%2 == 0)
			alloc := newAllocationAll("alloc-"+strconv.Itoa(i), "app-1", node.NodeID, "", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1}), false, 0)
			node.AddAllocation(alloc)
			node.RemoveAllocation("alloc-" + strconv.Itoa(i))
		}
	}()

	// the formatter: what the logging framework does with zap.Stringer("node", node)
	for i := 0; i < 2000; i++ {
		_ = node.String()
	}

	close(stop)
	wg.Wait()
}

// TestNodeStringUnderNodeLock formats a node while its own write lock is held, which is what
// Node.Reserve does through newReservation. String must not take the node lock: that would be a
// read lock on top of the write lock of the same goroutine and would never return.
func TestNodeStringUnderNodeLock(t *testing.T) {
	node := newNode("node-locked", map[string]resources.Quantity{"first": 100})

	done := make(chan string, 1)
	go func() {
		node.Lock()
		defer node.Unlock()
		done <- node.String()
	}()

	select {
	case out := <-done:
		assert.Assert(t, strings.Contains(out, "node-locked"), "the node id must be in the output: %s", out)
	case <-time.After(10 * time.Second):
		t.Fatal("String blocked while the node lock was held")
	}
}

// TestNodeStringContent pins the output: only the fields that are set when the node is created and
// never change may appear, everything else needs a lock this call cannot take.
func TestNodeStringContent(t *testing.T) {
	proto := &si.NodeInfo{
		NodeID:              "node-content",
		SchedulableResource: &si.Resource{Resources: map[string]*si.Quantity{"first": {Value: 100}}},
		Attributes: map[string]string{
			"si.io/hostname":    "host-1",
			"si.io/rackname":    "rack-1",
			"si/node-partition": "[rm-123]default",
		},
	}
	node := NewNode(proto)
	out := node.String()
	assert.Assert(t, strings.Contains(out, "node-content"), "the node id must be in the output: %s", out)
	assert.Assert(t, !strings.Contains(out, "Schedulable"), "the schedulable flag needs the lock: %s", out)
	assert.Assert(t, !strings.Contains(out, "Total"), "the total resource needs the lock: %s", out)
	assert.Assert(t, !strings.Contains(out, "Allocated"), "the allocated resource needs the lock: %s", out)
	assert.Assert(t, !strings.Contains(out, "allocations"), "the allocation count needs the lock: %s", out)

	var nilNode *Node
	assert.Equal(t, "node is nil", nilNode.String(), "a nil node still describes itself")
}
