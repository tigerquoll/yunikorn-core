//go:build !deadlock

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

package locking

// The methods below simply forward to the wrapped go-deadlock lock. They exist so that the
// gVisor checklocks analysis (see the "checklocks" make target) tracks the locking.Mutex and
// locking.RWMutex fields of a struct directly. Calling a promoted method of the wrapped lock
// instead makes the analysis attribute the acquisition to the inner field (i.e. "lock.mu"
// rather than "lock") and every "+checklocks:" field annotation fails to match.
//
// The go-deadlock lock is held in an unexported named field rather than embedded on purpose.
// Embedding promotes the methods this file does not forward, TryLock on both types plus
// TryRLock and RLocker on RWMutex. Those would silently bypass the forwarding again: the
// analysis would attribute them to the inner field, and the sync.Locker returned by RLocker
// is not tracked at all. With a named field any use of them is a compile error until a
// forwarder is added here deliberately.
//
// The "+checklocksignore" on each forwarder is required because a forwarder acquires (or
// releases) the inner lock and returns with the lock state changed, which is what the lock
// balance check flags. The ignore is not free: it also suppresses the per call site
// "already locked" and "unlock without lock" diagnostics for every wrapper lock in the code
// base. That class of bug is still caught at runtime by the go-deadlock detection. The rest
// of the lock state tracking is unaffected: guarded field access, the lock requirements of
// annotated functions and the return balance of the calling function are all still checked,
// the last of which is what reports an unlock-relock gap.
//
// The forwarders shift the go-deadlock reports by one stack frame: the "<<<<<" marker points
// at the forwarder in this file and the calling code is one frame further down. They are
// fully inlined so there is no runtime cost.

// +checklocksignore
//
// This is the default build: the lock class order check (see lockclass.go) is only compiled into
// the deadlock tagged build, so these forwarders are exactly the plain forwarding calls and stay
// inlinable. The instrumented copies live in forwarders_deadlock.go.

// +checklocksignore
func (m *Mutex) Lock() {
	m.mu.Lock()
}

// +checklocksignore
func (m *Mutex) Unlock() {
	m.mu.Unlock()
}

// +checklocksignore
func (m *RWMutex) Lock() {
	m.mu.Lock()
}

// +checklocksignore
func (m *RWMutex) Unlock() {
	m.mu.Unlock()
}

// +checklocksignore
func (m *RWMutex) RLock() {
	m.mu.RLock()
}

// +checklocksignore
func (m *RWMutex) RUnlock() {
	m.mu.RUnlock()
}
