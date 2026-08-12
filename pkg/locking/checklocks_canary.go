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

package locking

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
