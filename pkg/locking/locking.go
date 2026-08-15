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

// Package locking holds the lock wrappers of the scheduler and the order in which its lock
// classes may be taken, in the form the static analysis reads. The order is declared here, in
// the package that owns the lock types, so that every package holding one of these locks picks
// it up through its import rather than restating it. The classes themselves are declared on the
// types that carry the locks, see the "+lockclass" annotations there.
//
// The relation is a partial order and it is closed transitively: a pair the closure does not
// relate is not checked, because the taxonomy says nothing about it. Only the classes whose
// order the scheduler actually settles are declared; anything else stays classless.
//
// +lockorder:ClusterContext < PartitionContext
// +lockorder:PartitionContext < Application
// +lockorder:Application < Queue
// +lockorder:Application < Node
// +lockorder:Application < UGMManager
// +lockorder:UGMManager < UserTracker
// +lockorder:UGMManager < GroupTracker
//
// Queue is hierarchical: a queue walks its own tree, so two queues nest by design and the
// same class rule cannot apply to them. What must still hold is the direction, which
// "-lockorder.hierarchy" recovers from the parent link, see the "+lockhierarchyedge" on
// Queue.parent.
//
// +lockhierarchical:Queue
//
// Application is withheld rather than exempt: the same class rule is broken today by the
// cross application nesting of YUNIKORN-XXXX (the preemption victim scan and the required
// node reservation cancel both take a second application's lock while holding the first).
// Both are latent while scheduling runs on one goroutine, and both are fixed by hoisting the
// other application's work out of the critical section rather than by declaring an order
// between two locks of one class. The declaration is withheld so that this stays visible
// until then, and it is removed once those nestings are gone.
//
// +lockorderwithheld:Application
package locking

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	godeadlock "github.com/sasha-s/go-deadlock"

	"github.com/apache/yunikorn-core/pkg/log"
)

const (
	EnvDeadlockDetectionEnabled = "DEADLOCK_DETECTION_ENABLED"
	EnvDeadlockTimeoutSeconds   = "DEADLOCK_TIMEOUT_SECONDS"
	EnvExitOnDeadlock           = "DEADLOCK_EXIT"
	EnvDisableLockOrder         = "DEADLOCK_DISABLE_LOCK_ORDER"
)

var (
	once               sync.Once
	trackingEnabled    atomic.Bool
	timeoutSeconds     atomic.Int32
	deadlockDetected   atomic.Bool
	testingMode        atomic.Bool
	exitOnDeadlock     atomic.Bool
	disableOrderDetect atomic.Bool
)

type errorBuf struct {
	data string
	sync.Mutex
}

func (b *errorBuf) Write(p []byte) (n int, err error) {
	if b == nil {
		return len(p), nil
	}
	b.Lock()
	defer b.Unlock()
	b.data += string(p)
	return len(p), nil
}

func init() {
	once.Do(reInit)
}

func reInit() {
	enabled, err := strconv.ParseBool(os.Getenv(EnvDeadlockDetectionEnabled))
	if err != nil {
		enabled = false
	}
	trackingEnabled.Store(enabled)

	var timeoutSec int64
	timeoutSec, err = strconv.ParseInt(os.Getenv(EnvDeadlockTimeoutSeconds), 10, 32)
	if err != nil {
		timeoutSec = 60
	}
	timeoutSeconds.Store(int32(timeoutSec))

	var disableOrder bool
	disableOrder, err = strconv.ParseBool(os.Getenv(EnvDisableLockOrder))
	if err != nil {
		disableOrder = false
	}
	disableOrderDetect.Store(disableOrder)

	var exitOnDetect bool
	exitOnDetect, err = strconv.ParseBool(os.Getenv(EnvExitOnDeadlock))
	if err != nil {
		exitOnDetect = false
	}
	exitOnDeadlock.Store(exitOnDetect)

	// set deadlock detection options
	godeadlock.Opts.Disable = !enabled
	godeadlock.Opts.DeadlockTimeout = time.Duration(timeoutSec) * time.Second
	godeadlock.Opts.LogBuf = &errorBuf{}
	godeadlock.Opts.OnPotentialDeadlock = onPotentialDeadlock
	godeadlock.Opts.DisableLockOrderDetection = disableOrder

	if enabled {
		// We want to ensure that we write this before any other subsystem is initialized, including logging which may also use locks.
		// no way to handle errors just ignore
		_, _ = fmt.Fprintf(os.Stderr, "=== Deadlock detection enabled (timeout: %d seconds, exit on deadlock: %t, locking order disabled: %t) ===\n", timeoutSec, exitOnDetect, disableOrder)
	}
}

func onPotentialDeadlock() {
	deadlockDetected.Store(true)
	printBufContents()
	if exitOnDeadlock.Load() && !testingMode.Load() {
		os.Exit(1)
	}
}

func printBufContents() {
	buf, ok := godeadlock.Opts.LogBuf.(*errorBuf)
	buf.Lock()
	defer buf.Unlock()
	if !ok {
		log.Log(log.Diagnostics).Error("POTENTIAL DEADLOCK: No details available")
	} else {
		log.Log(log.Diagnostics).Error(buf.data)
	}
	buf.data = ""
}

func IsTrackingEnabled() bool {
	return trackingEnabled.Load()
}

func GetDeadlockTimeoutSeconds() int {
	return int(timeoutSeconds.Load())
}

func IsDeadlockDetected() bool {
	return deadlockDetected.Load()
}

// Mutex, and RWMutex below it, declare themselves lock primitives to the checklocks analysis.
// Without the declaration the analysis recognises a lock by its type name only, so the
// forwarders in forwarders.go read as ordinary methods that take a lock and return without
// releasing it and every one of them needs a "+checklocksignore" to silence that; see
// forwarders.go for what those ignores cost. Whether a type behaves as a Mutex or an RWMutex
// is taken from the type itself: it has an RLock method or it does not.
//
// +checklockslocktype
type Mutex struct {
	mu godeadlock.Mutex
}

// +checklockslocktype
type RWMutex struct {
	mu godeadlock.RWMutex
}
