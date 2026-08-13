//go:build checklocks_canary

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

// The edge below, and the two classes it relates, belong to this file alone. They are read
// only when the analyzer is handed this file, which no build ever is, so the taxonomy of the
// package is not affected by them.
//
// +lockorder:CanaryOuter < CanaryInner
package locking

import "strconv"

// This file is the canary for the checklocks analysis, it is not part of any build. The
// "checklocks" make target passes it to the analyzer explicitly, which ignores the build
// constraint above, and fails when the known violation below is not reported. Without it the
// check silently turns into a no-op the moment the analysis stops recognising our locks: the
// lock types are matched on their name, so renaming them, changing the way the forwarding
// methods below are written or a gvisor update are all enough to lose the coverage while
// every build stays green.
//
// The canary deliberately uses the full path that the rest of the code base uses: a wrapper
// lock from this package, held in a named field, with a field guarded by a "+checklocks:"
// annotation.
//
// One class of violation is covered per analyzer of the vet tool, and the make target checks
// for every message separately. The analyzers are independent of each other: an analyzer that
// stops reporting, or that is dropped from the command line, takes its own coverage with it
// and leaves the others green, which is exactly what this file exists to catch.
//
//	checklocks    a guarded field accessed without the lock, a call to a method that must
//	              not be called with the lock held, a lock taken twice on one path, and the
//	              same self deadlock reached from inside a callback whose lock is named by
//	              the value its body asserts: the second is the only machine check we have
//	              on the self deadlock rules, the third only holds while the wrapper types
//	              declare themselves locks, and the fourth only while a guard can name an
//	              asserted value, which is what the fsm callbacks are annotated with
//	lockstringer  a String method reading a guarded field, which races whatever fmt or zap
//	              was formatting it under
//	lockorder     an acquisition that takes the classes in the order the declaration
//	              forbids
//	lockblocking  a wait for something outside this process while a classed lock is held

type canary struct {
	// +checklocks:mu
	value int
	mu    RWMutex
}

// lockedWrite is correct, the lock is held for the write. It must not be reported.
func (c *canary) lockedWrite(value int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = value
}

// unlockedWrite is the violation the canary exists for: writing a guarded field without
// holding the lock. The analysis must report an invalid field access here.
func (c *canary) unlockedWrite(value int) {
	c.value = value
}

// selfLocking takes the lock itself, so holding it on entry would deadlock.
// +checklocksexclude:c.mu
func (c *canary) selfLocking(value int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = value
}

// reentrantCall is the second violation the canary exists for: calling a method that must
// not be entered with the lock held while holding it. The analysis must report that the lock
// must not be held here.
func (c *canary) reentrantCall(value int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.selfLocking(value)
}

// String is the lockstringer violation: fmt and zap call it at a point the type does not
// choose, so the guarded read below races any writer. The analysis must report a guarded read.
func (c *canary) String() string {
	return strconv.Itoa(c.value)
}

// relock takes and releases the lock twice over, which is balanced. It must not be reported.
func (c *canary) relock(value int) {
	c.mu.Lock()
	c.value = value
	c.mu.Unlock()
	c.mu.Lock()
	c.value = value
	c.mu.Unlock()
}

// doubleLock is the third violation the canary exists for: taking the same lock twice on one
// path, which self deadlocks. The analysis must report that the lock is already locked.
//
// This one is only reported while the wrapper types in locking.go declare themselves lock
// primitives. Without that declaration the forwarding methods need a "+checklocksignore"
// each, and an ignore is read at every call site of the function that carries it, so the
// whole class disappears for every wrapper lock in the code base with the canary staying
// green on the strength of the other messages. That is what this fixture is here to catch.
func (c *canary) doubleLock(value int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.Lock()
	c.value = value
}

// callbackCanary is the subject of the callback fixture below. Its guarded field is named
// apart from the one above so that a diagnostic about it can only have come from there.
type callbackCanary struct {
	// +checklocks:mu
	callbackValue int
	mu            RWMutex
}

// callbackSelfLocking takes the subject's own lock, so holding it on entry would deadlock.
// +checklocksexclude:c.mu
func (c *callbackCanary) callbackSelfLocking(value int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callbackValue = value
}

// callbackEvent stands in for what a state machine library hands a callback: the subject
// arrives inside an interface and the body recovers it by asserting a type.
type callbackEvent struct {
	Args []any
}

// callbackTable is the shape the fsm callbacks in this code base have: a table of literals
// handed to a library, each stating the lock its caller holds by naming the value its own
// body recovers, because that value exists nowhere else to be named.
//
// Both polarities are in the one literal. The write through the asserted subject is correct
// and must never be reported, and the call below it must be, because the guard put that
// subject's lock in scope and the callee takes it again.
//
// The second of those is what the make target requires, and it is the only message here that
// a guard which stopped binding would take with it: a guard matching nothing records no lock,
// silently, and then the call is fine while the write is reported instead. Requiring the
// report on the WRITE would pass in both worlds and prove nothing.
func callbackTable() map[string]func(*callbackEvent) {
	return map[string]func(*callbackEvent){
		// +checklocks:event.Args[0].(*callbackCanary).mu
		"enter": func(event *callbackEvent) {
			subject := event.Args[0].(*callbackCanary)
			subject.callbackValue = 1
			subject.callbackSelfLocking(2)
		},
	}
}

// canaryOuter and canaryInner carry the two classes of the edge declared at the top of this
// file, in the two shapes the code base uses: a named lock field and an embedded lock.
//
// +lockclass:CanaryOuter
type canaryOuter struct {
	mu RWMutex
}

// +lockclass:CanaryInner
type canaryInner struct {
	RWMutex
}

// downwardAcquire takes the two classes in the declared order. It must not be reported.
func downwardAcquire(o *canaryOuter, i *canaryInner) {
	o.mu.Lock()
	defer o.mu.Unlock()
	i.Lock()
	defer i.Unlock()
}

// upwardAcquire is the lockorder violation: the outer class is declared before the inner one,
// so taking it while the inner one is held is the inversion. The analysis must report the
// declared order here.
func upwardAcquire(o *canaryOuter, i *canaryInner) {
	i.Lock()
	defer i.Unlock()
	o.mu.Lock()
	defer o.mu.Unlock()
}

// nonBlockingSend hands the value over without waiting, the dispatch idiom. It must not be
// reported: a select with a default never waits.
func (o *canaryOuter) nonBlockingSend(reply chan int, value int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	select {
	case reply <- value:
	default:
	}
}

// blockingReceive is the lockblocking violation: the lock is held while waiting for an answer
// that may never come. The analysis must report a wait under a lock.
func (o *canaryOuter) blockingReceive(reply chan int) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return <-reply
}
