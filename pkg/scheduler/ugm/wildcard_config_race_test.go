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

package ugm

import (
	"strconv"
	"sync"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
)

// TestWildCardConfigReloadDuringScheduling reloads the configuration while the scheduling hot path
// walks the queue hierarchy. Every walk that reaches a queue without a tracker creates one, and
// the creation used to read the wild card limits straight from the manager, which the reload
// replaces wholesale.
//
// Run with -race. On the unfixed code this reports a data race between the map read in
// getUserWildCardLimitsConfig and the map replacement in replaceLimitConfigs.
func TestWildCardConfigReloadDuringScheduling(t *testing.T) {
	setupUGM()
	manager := GetUserManager()
	user := security.UserGroup{User: "test-user", Groups: []string{"test-group"}}
	conf := createUpdateConfig(user.User, user.Groups[0])

	var wg sync.WaitGroup
	iterations := 200

	// the configuration reload: replaces the limit maps under the manager lock
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
		}
	}()

	// the scheduling hot path: a queue path that has no tracker yet on every iteration, so the
	// walk has to create the queue trackers as it descends
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			queuePath := "root.parent.child" + strconv.Itoa(i)
			manager.Headroom(queuePath, "app-"+strconv.Itoa(i), user)
			manager.CanRunApp(queuePath, "app-"+strconv.Itoa(i), user)
		}
	}()

	wg.Wait()
}

// TestWildCardConfigReloadDuringIncrease is the same race on the resource tracking path.
func TestWildCardConfigReloadDuringIncrease(t *testing.T) {
	setupUGM()
	manager := GetUserManager()
	user := security.UserGroup{User: "test-user", Groups: []string{"test-group"}}
	conf := createUpdateConfig(user.User, user.Groups[0])
	usage, err := resources.NewResourceFromConf(map[string]string{"memory": "10", "vcores": "10"})
	assert.NilError(t, err)

	var wg sync.WaitGroup
	iterations := 200

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			queuePath := "root.parent.increase" + strconv.Itoa(i)
			manager.IncreaseTrackedResource(queuePath, "app-"+strconv.Itoa(i), usage, user)
		}
	}()

	wg.Wait()
}
