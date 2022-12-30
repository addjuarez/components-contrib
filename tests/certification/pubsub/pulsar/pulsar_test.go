/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pulsar_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	//"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub/Sub.

	pubsub_pulsar "github.com/dapr/components-contrib/pubsub/pulsar"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"

	// Dapr runtime and Go-SDK
	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"
	//kit_retry "github.com/dapr/kit/retry"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	sidecarName1      = "dapr-1"
	sidecarName2      = "dapr-2"
	appID1            = "app-1"
	appID2            = "app-2"
	clusterName       = "pulsarcertification"
	dockerComposeYAML = "docker-compose.yml"
	numMessages       = 1000
	appPort           = 8000
	portOffset        = 2
	messageKey        = "partitionKey"

	pubsubName = "messagebus"
	topicName  = "neworder"
)

var brokers = []string{"localhost:19092", "localhost:29092", "localhost:39092"}

func TestPulsar(t *testing.T) {
	// For Pulsar, we should ensure messages are received in order.
	consumerGroup1 := watcher.NewOrdered()
	// This watcher is across multiple consumers in the same group
	// so exact ordering is not expected.
	consumerGroup2 := watcher.NewUnordered()

	// Application logic that tracks messages from a topic.
	application := func(appName string, watcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)

			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: "messagebus",
					Topic:      "neworder",
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}
					ctx.Logf("======== %s received event: %s", appName, e.Data)

					// Track/Observe the data of the event.
					watcher.Observe(e.Data)
					return false, nil
				}),
			)
		}
	}

	// Set the partition key on all messages so they
	// are written to the same partition.
	// This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: "test",
	}

	// Test logic that sends messages to a topic and
	// verifies the application has received them.
	sendRecvTest := func(metadata map[string]string, watchers ...*watcher.Watcher) flow.Runnable {
		_, hasKey := metadata[messageKey]
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)

			// Declare what is expected BEFORE performing any steps
			// that will satisfy the test.
			msgs := make([]string, numMessages)
			for i := range msgs {
				msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
			}
			for _, m := range watchers {
				m.ExpectStrings(msgs...)
			}
			// If no key it provided, create a random one.
			// For Pulsar, this will spread messages across
			// the topic's partitions.
			if !hasKey {
				metadata[messageKey] = uuid.NewString()
			}

			// Send events that the application above will observe.
			ctx.Log("Sending messages!")
			for _, msg := range msgs {
				err := client.PublishEvent(
					ctx, pubsubName, topicName, msg,
					dapr.PublishEventWithMetadata(metadata))
				require.NoError(ctx, err, "error publishing message")
			}

			// Do the messages we observed match what we expect?
			for _, m := range watchers {
				m.Assert(ctx, time.Minute)
			}

			return nil
		}
	}

	flow.New(t, "pulsar certification").
		// Run Pulsar using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for broker sockets",
			network.WaitForAddresses(5*time.Minute, brokers...)).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			config := sarama.NewConfig()
			config.ClientID = "test-consumer"
			config.Consumer.Return.Errors = true

			// Create new consumer
			client, err := sarama.NewConsumer(brokers, config)
			if err != nil {
				return err
			}
			defer client.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			_, err = client.ConsumePartition("myTopic", 0, sarama.OffsetOldest)

			return err
		})).
		//
		// Run the application logic above.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			application(appID1, consumerGroup1))).
		//
		// Run the Dapr sidecar with the Pulsar component.
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer1"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			componentRuntimeOptions(),
		)).
		//
		// Run the second application.
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			application(appID2, consumerGroup2))).
		//
		// Run the Dapr sidecar with the Pulsar component.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer2"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		//
		// Send messages using the same metadata/message key so we can expect
		// in-order processing.
		Step("send and wait(in-order)", sendRecvTest(metadata, consumerGroup1, consumerGroup2)).
		Run()
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_pulsar.NewPulsar, "pulsar")

	return []runtime.Option{
		runtime.WithPubSubs(pubsubRegistry),
	}
}
