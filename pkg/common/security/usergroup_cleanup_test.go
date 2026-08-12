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

package security

import (
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

// withoutSingleton clears the package globals for the duration of a test and restores them, so a
// test can work with a cache that is not the shared instance.
func withoutSingleton(t *testing.T) {
	t.Helper()
	oldInstance := instance
	oldOnce := once
	oldStopped := stopped.Load()
	instance = nil
	once = &sync.Once{}
	stopped.Store(false)
	t.Cleanup(func() {
		instance = oldInstance
		once = oldOnce
		stopped.Store(oldStopped)
	})
}

// expiredCache returns a cache holding one entry that the cleaner should remove and one it should
// keep.
func expiredCache() *UserGroupCache {
	cache := GetUserGroupCacheTest()
	cache.ugs["expired"] = &UserGroup{
		User:     "expired",
		resolved: time.Now().Unix() - poscache - 10,
	}
	cache.ugs["current"] = &UserGroup{
		User:     "current",
		resolved: time.Now().Unix(),
	}
	return cache
}

// TestCleanUpCacheWithoutInstance cleans a cache that is not the shared instance. The cleanup used
// to lock the instance rather than the cache it was called on, so with no instance published it
// dereferenced nil, and with one published it would have locked the wrong cache.
func TestCleanUpCacheWithoutInstance(t *testing.T) {
	withoutSingleton(t)
	cache := expiredCache()

	cache.cleanUpCache()

	assert.Equal(t, 1, cache.getUGsize(), "only the expired entry should be removed: %v", cache.getUGmap())
	_, ok := cache.getUGmap()["current"]
	assert.Assert(t, ok, "the current entry must be kept")
}

// TestResetCacheWithoutInstance is the same for the reset used by the tests themselves.
func TestResetCacheWithoutInstance(t *testing.T) {
	withoutSingleton(t)
	cache := expiredCache()

	cache.resetCache()

	assert.Equal(t, 0, cache.getUGsize(), "the cache must be empty after a reset")
}

// TestCleanUpCacheUsesReceiverLock checks that the cleanup locks the cache it works on: with the
// cache's own lock held the cleanup must not be able to run.
func TestCleanUpCacheUsesReceiverLock(t *testing.T) {
	withoutSingleton(t)
	cache := expiredCache()

	cache.lock.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		cache.cleanUpCache()
	}()

	select {
	case <-done:
		cache.lock.Unlock()
		t.Fatal("cleanUpCache did not take the lock of the cache it was called on")
	case <-time.After(100 * time.Millisecond):
	}
	cache.lock.Unlock()
	<-done
}

// TestStopDuringCleanerRun stops the cache while its cleaner is running. Stop clears the global
// instance, and a cleaner that is past the select and inside the cleanup must not be looking at
// that global any more.
//
// Run with -race: the cleanup used to read the instance without synchronisation while Stop wrote
// it, and a cleaner that got there after Stop crashed on the nil.
func TestStopDuringCleanerRun(t *testing.T) {
	withoutSingleton(t)

	for i := 0; i < 20; i++ {
		cache := expiredCache()
		cache.interval = time.Millisecond
		instance = cache
		stopped.Store(false)
		go cache.run()

		// let the cleaner tick a few times before pulling the instance out from under it
		time.Sleep(5 * time.Millisecond)
		cache.Stop()
		// and give any cleanup that was already running the chance to finish
		time.Sleep(5 * time.Millisecond)
	}
}
