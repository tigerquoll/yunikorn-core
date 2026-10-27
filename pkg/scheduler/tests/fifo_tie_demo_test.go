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

package tests

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	demoNode = "worker-1"
	podMem   = int64(4 * 1024 * 1024 * 1024)
)

func podResource(vcore, mem int64) *si.Resource {
	return &si.Resource{Resources: map[string]*si.Quantity{
		siCommon.CPU:    {Value: vcore},
		siCommon.Memory: {Value: mem},
		"pods":          {Value: 1},
	}}
}

func submitApp(t *testing.T, ms *mockScheduler, appID, ns string) {
	err := ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New: []*si.AddApplicationRequest{{
			ApplicationID: appID,
			QueueName:     "", // no queue label: the placement rule decides
			PartitionName: "default",
			Ugi:           &si.UserGroupInformation{User: "system:serviceaccount:kube-system:job-controller"},
			Tags: map[string]string{
				"namespace":                ns,
				siCommon.AppTagCreateForce: "false",
			},
		}},
		RmID: ms.rmID,
	})
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedApplication(t, appID, 2000)
}

// a UID shaped allocation key: pod UIDs are unique per pod, so no two jobs share a key
func podUID(jobName string, i int) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(jobName))
	return fmt.Sprintf("%08x-5a1e-4c2b-9f00-%012d", h.Sum32(), i)
}

// asks carry the fields the shim's CreateAllocationForTask sets that the FIFO sort reads:
// the pod creation time in whole seconds and the pod priority (0 when unset)
func submitPods(t *testing.T, ms *mockScheduler, appID, ns, jobName string, n int, created int64) []string {
	keys := make([]string, 0, n)
	asks := make([]*si.Allocation, 0, n)
	for i := 0; i < n; i++ {
		uid := podUID(jobName, i)
		keys = append(keys, uid)
		asks = append(asks, &si.Allocation{
			AllocationKey: uid,
			ApplicationID: appID,
			AllocationTags: map[string]string{
				siCommon.DomainK8s + siCommon.GroupMeta + siCommon.KeyNamespace: ns,
				siCommon.DomainK8s + siCommon.GroupMeta + siCommon.KeyPodName:   fmt.Sprintf("%s-%d-x%dz", jobName, i, i),
				siCommon.CreationTime: strconv.FormatInt(created, 10),
			},
			ResourcePerAlloc: podResource(1000, podMem),
			Priority:         0,
			PreemptionPolicy: &si.PreemptionPolicy{AllowPreemptSelf: true, AllowPreemptOther: true},
		})
	}
	err := ms.proxy.UpdateAllocation(&si.AllocationRequest{Allocations: asks, RmID: ms.rmID})
	assert.NilError(t, err)
	return keys
}

func releasePod(t *testing.T, ms *mockScheduler, appID, key string) {
	err := ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{AllocationsToRelease: []*si.AllocationRelease{{
			PartitionName:   "default",
			ApplicationID:   appID,
			AllocationKey:   key,
			TerminationType: si.TerminationType_STOPPED_BY_RM,
			Message:         "task completed",
		}}},
		RmID: ms.rmID,
	})
	assert.NilError(t, err)
}

// a non-YuniKorn pod as the shim reports it in CreateAllocationForForeignPod: no application, foreign tag, node set
func addForeignPod(t *testing.T, ms *mockScheduler, nodeID, ns, podName string, created int64) {
	err := ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{{
			AllocationKey: podUID(podName, 0),
			AllocationTags: map[string]string{
				siCommon.DomainK8s + siCommon.GroupMeta + siCommon.KeyNamespace: ns,
				siCommon.DomainK8s + siCommon.GroupMeta + siCommon.KeyPodName:   podName,
				siCommon.Foreign:      siCommon.AllocTypeDefault,
				siCommon.CreationTime: strconv.FormatInt(created, 10),
			},
			ResourcePerAlloc: podResource(1000, podMem),
			NodeID:           nodeID,
		}},
		RmID: ms.rmID,
	})
	assert.NilError(t, err)
}

func allocatedPods(ms *mockScheduler, appID string) int {
	app := ms.getApplication(appID)
	if app == nil {
		return -1
	}
	return int(app.GetAllocatedResource().Resources[siCommon.CPU] / 1000)
}

// A 4-pod "etl" job fills the four vcore a system pod leaves free on a 5 vcore node while
// two 4-pod "ml" jobs wait for it, train-a submitted before train-b; the etl pods then
// finish one by one. With distinct submission times FIFO gives all four freed slots to the
// older job. The Jobs carry an applicationId label, as the YuniKorn docs require to keep
// them separate apps.
// Kubernetes creationTimestamp has a one-second resolution and the shim passes it on as
// Unix seconds (CreateAllocationForTask sets the CreationTime tag to
// pod.CreationTimestamp.Unix(), yunikorn-k8shim pkg/common/si_helper.go:109), so jobs
// created in the same second tie on the FIFO sort key. Stock configs.DefaultSchedulerConfig.
func runFifoTie(t *testing.T, gapSeconds int64) {
	ms := &mockScheduler{}
	defer ms.Stop()
	assert.NilError(t, ms.Init(configs.DefaultSchedulerConfig, false, false))

	assert.NilError(t, ms.addNode(demoNode, &si.Resource{Resources: map[string]*si.Quantity{
		siCommon.CPU:    {Value: 5000},
		siCommon.Memory: {Value: 5 * podMem},
		"pods":          {Value: 110},
	}}))
	ms.mockRM.waitForAcceptedNode(t, demoNode, 2000)
	cc := ms.scheduler.GetClusterContext()

	now := time.Now().Unix()
	// a real node runs system and daemonset pods the scheduler never placed
	addForeignPod(t, ms, demoNode, "kube-system", "node-exporter-7xk2p", now-3600)
	waitForAvailableNodeResource(t, cc, partition, []string{demoNode}, resources.Quantity(4*podMem), 2000)

	// running etl job, started 10 minutes ago
	submitApp(t, ms, "etl-hourly", "etl")
	etlKeys := submitPods(t, ms, "etl-hourly", "etl", "etl-hourly", 4, now-600)
	ms.scheduler.MultiStepSchedule(5)
	waitForAllocatedNodeResource(t, cc, partition, []string{demoNode}, resources.Quantity(4*podMem), 2000)

	// two training jobs from one manifest, pods pending for 30s
	created := now - 30
	submitApp(t, ms, "train-a", "ml")
	submitApp(t, ms, "train-b", "ml")
	submitPods(t, ms, "train-a", "ml", "train-a", 4, created)
	submitPods(t, ms, "train-b", "ml", "train-b", 4, created+gapSeconds)
	waitForPendingQueueResource(t, ms.getQueue("root.ml"), resources.Quantity(8*podMem), 2000)
	ms.scheduler.MultiStepSchedule(3) // nothing fits
	t.Logf("reservations train-a=%d train-b=%d",
		len(ms.getApplication("train-a").GetReservations()), len(ms.getApplication("train-b").GetReservations()))

	order := ""
	prevA, prevB := 0, 0
	for _, key := range etlKeys {
		releasePod(t, ms, "etl-hourly", key)
		waitForAllocatedNodeResource(t, cc, partition, []string{demoNode}, resources.Quantity(3*podMem), 2000)
		ms.scheduler.MultiStepSchedule(3)
		waitForAllocatedNodeResource(t, cc, partition, []string{demoNode}, resources.Quantity(4*podMem), 2000)
		a, b := allocatedPods(ms, "train-a"), allocatedPods(ms, "train-b")
		if a > prevA {
			order += "A"
		}
		if b > prevB {
			order += "B"
		}
		prevA, prevB = a, b
	}
	assert.NilError(t, ms.removeApp("etl-hourly", "default"))

	a, b := allocatedPods(ms, "train-a"), allocatedPods(ms, "train-b")
	t.Logf("slot order=%s train-a=%d train-b=%d", order, a, b)
	assert.Assert(t, (a == 4 && b == 0) || (b == 4 && a == 0),
		"jobs were not serialized: train-a=%d train-b=%d (slot order %s)", a, b, order)
}

func TestFifoTieSameSecond(t *testing.T) { runFifoTie(t, 0) }

func TestFifoTieOneSecondApart(t *testing.T) { runFifoTie(t, 1) }
