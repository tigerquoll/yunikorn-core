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

package locking

// The methods below are the deadlock tagged copies of the forwarders in forwarders.go: why the
// wrappers forward at all, and why the wrapped lock is a named field rather than embedded, is
// written down there once and not repeated here.
//
// What belongs to this build is that the forwarders also drive the lock class order check. The
// check is behind an atomic switch that is only set when DEADLOCK_CLASS_ORDER_ENABLED is on, and
// its body is a call so these copies do not inline. That is why the default build in forwarders.go
// carries the plain versions.

func (m *Mutex) Lock() {
	if classOrderEnabled.Load() {
		m.enterClass()
	}
	m.mu.Lock()
}

func (m *Mutex) Unlock() {
	if classOrderEnabled.Load() {
		m.leaveClass()
	}
	m.mu.Unlock()
}

func (m *RWMutex) Lock() {
	if classOrderEnabled.Load() {
		m.enterClass()
	}
	m.mu.Lock()
}

func (m *RWMutex) Unlock() {
	if classOrderEnabled.Load() {
		m.leaveClass()
	}
	m.mu.Unlock()
}

func (m *RWMutex) RLock() {
	if classOrderEnabled.Load() {
		m.enterClass()
	}
	m.mu.RLock()
}

func (m *RWMutex) RUnlock() {
	if classOrderEnabled.Load() {
		m.leaveClass()
	}
	m.mu.RUnlock()
}
