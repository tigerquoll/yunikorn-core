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
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/plugins"
)

// stores the push event internal
var defaultPushEventInterval = 2 * time.Second

type eventPublisher struct {
	store             *EventStore
	pushEventInterval time.Duration
	stopCh            chan struct{}
	stopped           atomic.Bool
}

func createShimPublisher(store *EventStore) *eventPublisher {
	publisher := &eventPublisher{
		store:             store,
		pushEventInterval: defaultPushEventInterval,
	}
	publisher.stopped.Store(true)
	return publisher
}

func (sp *eventPublisher) start() {
	log.Log(log.Events).Info("Starting event publisher")
	// handle a restart correctly
	if !sp.stopped.CompareAndSwap(true, false) {
		log.Log(log.Events).Info("Event publisher already running")
		return
	}
	stopCh := make(chan struct{})
	sp.stopCh = stopCh
	// the routine is given the channel it stops on, reading it from the field would race with
	// stop(), which clears it, and with a following start(), which replaces it
	go func(stopCh <-chan struct{}) {
		for {
			select {
			case <-stopCh:
				log.Log(log.Events).Info("Event publisher exiting")
				return
			case <-time.After(sp.pushEventInterval):
				messages := sp.store.CollectEvents()
				if len(messages) > 0 {
					if eventPlugin := plugins.GetResourceManagerCallbackPlugin(); eventPlugin != nil {
						log.Log(log.Events).Debug("Sending eventChannel",
							zap.Int("number of messages", len(messages)))
						eventPlugin.SendEvent(messages)
					}
				}
			}
		}
	}(stopCh)
}

func (sp *eventPublisher) stop() {
	if !sp.stopped.CompareAndSwap(false, true) {
		log.Log(log.Events).Info("Event publisher already stopped")
		return
	}
	log.Log(log.Events).Info("Stopping event publisher")
	// closing rather than sending: the routine can be inside the shim callback handing events
	// over, and a send would wait for it to come back to its select. The channel is not reused,
	// start() makes a new one.
	close(sp.stopCh)
	sp.stopCh = nil
}

func (sp *eventPublisher) getEventStore() *EventStore {
	return sp.store
}
