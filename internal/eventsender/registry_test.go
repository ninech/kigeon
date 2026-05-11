package eventsender_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ninech/kigeon/internal/eventqueue"
	"github.com/ninech/kigeon/internal/eventsender"
)

// stubSender is a minimal EventSender used by factory stubs.
type stubSender struct{ name string }

func (s *stubSender) Name() string                { return s.name }
func (s *stubSender) Run(_ context.Context) error { return nil }
func (s *stubSender) Stop()                       {}

func TestCreate_unknownType(t *testing.T) {
	_, err := eventsender.Create("test-unknown-abc123", "my-sender", nil, nil, eventsender.FactoryOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown sender type")
}

func TestRegister_nilFactoryPanics(t *testing.T) {
	assert.Panics(t, func() {
		eventsender.Register("test-nil-factory-xyz789", nil)
	})
}

func TestRegister_duplicatePanics(t *testing.T) {
	factory := func(name string, _ json.RawMessage, _ *eventqueue.EventFetcher, _ eventsender.FactoryOptions) (eventsender.EventSender, error) {
		return &stubSender{name: name}, nil
	}
	eventsender.Register("test-dup-type-qwerty1", factory)
	assert.Panics(t, func() {
		eventsender.Register("test-dup-type-qwerty1", factory)
	})
}

func TestRegisteredTypes_includesFreshType(t *testing.T) {
	const typeName = "test-fresh-type-poiuy2"
	factory := func(name string, _ json.RawMessage, _ *eventqueue.EventFetcher, _ eventsender.FactoryOptions) (eventsender.EventSender, error) {
		return &stubSender{name: name}, nil
	}
	eventsender.Register(typeName, factory)

	types := eventsender.RegisteredTypes()
	assert.Contains(t, types, typeName)
}

func TestCreate_callsFactoryWithCorrectName(t *testing.T) {
	const typeName = "test-factory-call-asdfg3"
	var capturedName string

	factory := func(name string, _ json.RawMessage, _ *eventqueue.EventFetcher, _ eventsender.FactoryOptions) (eventsender.EventSender, error) {
		capturedName = name
		return &stubSender{name: name}, nil
	}
	eventsender.Register(typeName, factory)

	sender, err := eventsender.Create(typeName, "expected-sender-name", nil, nil, eventsender.FactoryOptions{})
	require.NoError(t, err)
	assert.Equal(t, "expected-sender-name", capturedName)
	assert.Equal(t, "expected-sender-name", sender.Name())
}
