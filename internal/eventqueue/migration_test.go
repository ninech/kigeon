package eventqueue

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TestStartEventQueue_MigratesLimitsRetention ensures that a stream created
// by earlier versions (using limits retention) is switched to interest
// retention, dropping events which were already acknowledged.
func TestStartEventQueue_MigratesLimitsRetention(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()

	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: dataDir})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second))
	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	stream, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{subjectName},
		Storage:  jetstream.FileStorage,
	})
	require.NoError(t, err)
	consumer, err := stream.CreateOrUpdateConsumer(t.Context(), jetstream.ConsumerConfig{
		Durable:   "sender",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)

	for range 3 {
		_, err := js.Publish(t.Context(), subjectName, []byte("{}"))
		require.NoError(t, err)
	}
	// acknowledge the first two events only
	batch, err := consumer.Fetch(2)
	require.NoError(t, err)
	for msg := range batch.Messages() {
		require.NoError(t, msg.DoubleAck(t.Context()))
	}
	require.NoError(t, batch.Error())
	nc.Close()
	ns.Shutdown()
	ns.WaitForShutdown()

	eq, err := StartEventQueue(t.Context(), dataDir, Options{
		EventsMaxAge:   time.Hour,
		EventsMaxBytes: resource.MustParse("100Mi"),
	})
	require.NoError(t, err)
	defer eq.Stop()

	info, err := eq.jetStreamStream.Info(t.Context())
	require.NoError(t, err)
	assert.Equal(t, jetstream.InterestPolicy, info.Config.Retention)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		got, err := eq.StreamMessages(context.Background())
		require.NoError(c, err)
		assert.Equal(c, uint64(1), got)
	}, 5*time.Second, 50*time.Millisecond)
}
