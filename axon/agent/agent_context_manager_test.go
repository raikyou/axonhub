package agent

import (
	"context"
	"fmt"
	"sync"
	"testing"

	axoncontext "github.com/looplj/axonhub/axon/context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type contextTestProvider struct {
	mu       sync.Mutex
	count    int
	inputs   [][]Message
	usagePer Usage
}

func (p *contextTestProvider) Chat(_ context.Context, _ string, _ []ToolDefinition, messages []Message) (Response, error) {
	p.mu.Lock()
	p.inputs = append(p.inputs, cloneMessages(messages))
	p.count++
	count := p.count
	p.mu.Unlock()

	text := fmt.Sprintf("assistant-%d", count)
	return Response{
		Messages: []Message{{Role: RoleAssistant, Content: &Content{Text: &text}}},
		Usage:    p.usagePer,
	}, nil
}

func (p *contextTestProvider) ChatStream(_ context.Context, _ string, _ []ToolDefinition, _ []Message) (<-chan StreamEvent, error) {
	return nil, fmt.Errorf("not implemented")
}

func TestAgentWithContextManager_CompactsHistoryAndTracksUsage(t *testing.T) {
	store := NewContextManagerMemoryStore()
	cfg := DefaultContextManagerConfig()
	cfg.MaxRecentMessages = 2
	cm, err := NewPersistentContextManager(cfg, store)
	require.NoError(t, err)

	provider := &contextTestProvider{usagePer: Usage{InputTokens: 11, OutputTokens: 5}}
	a := New(Config{Model: "test-model", MaxIterations: 5}, provider, WithContextManager(cm))

	ctx := axoncontext.WithThreadID(context.Background(), "thread-a")

	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf("user-%d", i+1)
		err := a.Process(ctx, Content{Text: &msg})
		require.NoError(t, err)
	}

	history := a.Messages()
	assert.LessOrEqual(t, len(history), 3)

	snap := cm.Snapshot()
	assert.Equal(t, int64(33), snap.TotalInputTokens)
	assert.Equal(t, int64(15), snap.TotalOutputTokens)

	require.Len(t, provider.inputs, 3)
	thirdCall := provider.inputs[2]
	require.NotEmpty(t, thirdCall)
	assert.Equal(t, RoleSystem, thirdCall[0].Role)
	assert.Contains(t, thirdCall[0].Content.String(), "Summary of earlier conversation")
}

var _ Provider = (*contextTestProvider)(nil)
