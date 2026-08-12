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

package events

import (
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/mock"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func stopTestEvent() *si.EventRecord {
	return &si.EventRecord{
		Type:              si.EventRecord_REQUEST,
		ObjectID:          "alloc",
		ReferenceID:       "app",
		Message:           "stop test",
		TimestampNano:     time.Now().UnixNano(),
		EventChangeType:   si.EventRecord_ADD,
		EventChangeDetail: si.EventRecord_DETAILS_NONE,
	}
}

// TestStopWhileServiceRunning restarts the event system while events are added. The service
// routine reads the stop and event channels, both of which Stop and StartServiceWithPublisher
// write under the lock, so the routine must not read them from the fields.
//
// Run with -race. On the unfixed code this reports a data race on those fields.
func TestStopWhileServiceRunning(t *testing.T) {
	Init()
	eventSystem, ok := GetEventSystem().(*EventSystemImpl)
	assert.Assert(t, ok, "event system is not the expected implementation")

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			eventSystem.AddEvent(stopTestEvent())
		}
	}()

	for i := 0; i < 20; i++ {
		eventSystem.StartServiceWithPublisher(false)
		eventSystem.Stop()
	}
	close(stop)
	wg.Wait()
}

// TestStopWithBlockedPublisher stops the event system while the publisher routine is blocked in
// the shim callback. The mock plugin takes a channel of three, so a publisher that sends more
// than that blocks in SendEvent and never returns to its select.
//
// Stop must not wait for that routine, and above all must not wait for it while holding the write
// lock: every AddEvent takes the read lock, so the event system as a whole would stop.
func TestStopWithBlockedPublisher(t *testing.T) {
	defer plugins.UnregisterSchedulerPlugins()
	plugins.RegisterSchedulerPlugin(mock.NewEventPlugin())

	interval := defaultPushEventInterval
	defaultPushEventInterval = time.Millisecond
	defer func() { defaultPushEventInterval = interval }()

	Init()
	eventSystem, ok := GetEventSystem().(*EventSystemImpl)
	assert.Assert(t, ok, "event system is not the expected implementation")
	eventSystem.StartService()

	// more events than the mock plugin can take, the publisher blocks handing them over
	for i := 0; i < 20; i++ {
		eventSystem.AddEvent(stopTestEvent())
	}
	// let the publisher pick the events up and wedge in the plugin
	time.Sleep(100 * time.Millisecond)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		eventSystem.Stop()
	}()

	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return while the publisher was blocked in the shim callback")
	}

	// and the event system is still usable: AddEvent takes the read lock, which a Stop waiting
	// under the write lock would hold forever
	done := make(chan struct{})
	go func() {
		defer close(done)
		eventSystem.AddEvent(stopTestEvent())
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("AddEvent blocked after Stop")
	}
}

// TestStopRestartWithBlockedPublisher covers the restart path of the config reload with a blocked
// publisher: the system must come back up rather than wedge, and the new publisher must get its
// own stop channel.
func TestStopRestartWithBlockedPublisher(t *testing.T) {
	defer plugins.UnregisterSchedulerPlugins()
	plugins.RegisterSchedulerPlugin(mock.NewEventPlugin())

	interval := defaultPushEventInterval
	defaultPushEventInterval = time.Millisecond
	defer func() { defaultPushEventInterval = interval }()

	Init()
	eventSystem, ok := GetEventSystem().(*EventSystemImpl)
	assert.Assert(t, ok, "event system is not the expected implementation")
	eventSystem.StartService()
	for i := 0; i < 20; i++ {
		eventSystem.AddEvent(stopTestEvent())
	}
	time.Sleep(100 * time.Millisecond)

	restarted := make(chan struct{})
	go func() {
		defer close(restarted)
		eventSystem.restart()
	}()
	select {
	case <-restarted:
	case <-time.After(10 * time.Second):
		t.Fatal("restart did not complete while the publisher was blocked in the shim callback")
	}

	eventSystem.RLock()
	publisher := eventSystem.publisher
	channel := eventSystem.channel
	eventSystem.RUnlock()
	assert.Assert(t, publisher != nil, "the restart must leave a publisher behind")
	assert.Assert(t, channel != nil, "the restart must leave an event channel behind")
	assert.Assert(t, !eventSystem.stopped.Load(), "the event system must be running after a restart")

	eventSystem.Stop()
}
