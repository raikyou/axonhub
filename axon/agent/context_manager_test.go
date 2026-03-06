package agent

import (
	"context"
	"path/filepath"
	"testing"

	axoncontext "github.com/looplj/axonhub/axon/context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextManagerPrepare_CompactsAndKeepsRecentHistory(t *testing.T) {
	store := NewContextManagerMemoryStore()
	cfg := DefaultContextManagerConfig()
	cfg.MaxRecentMessages = 2
	cfg.CrossThreadSummaryCount = 1

	cm, err := NewPersistentContextManager(cfg, store)
	require.NoError(t, err)

	ctx := axoncontext.WithThreadID(context.Background(), "thread-a")
	history := []Message{
		newTextMessage(RoleUser, "u1"),
		newTextMessage(RoleAssistant, "a1"),
		newTextMessage(RoleUser, "u2"),
		newTextMessage(RoleAssistant, "a2"),
	}
	cm.AddMessages(ctx, history...)

	res := cm.Prepare(ctx, cm.Messages(ctx))
	require.True(t, res.Compacted)
	require.Len(t, res.PrunedHistory, 2)
	require.GreaterOrEqual(t, len(res.Messages), 3)
	assert.Equal(t, RoleSystem, res.Messages[0].Role)

	snapshot := cm.Snapshot()
	require.Contains(t, snapshot.Threads, "thread-a")
	assert.NotEmpty(t, snapshot.Threads["thread-a"].Summary)
}

func TestContextManagerPrepare_IncludesCrossThreadSummary(t *testing.T) {
	store := NewContextManagerMemoryStore()
	cfg := DefaultContextManagerConfig()
	cfg.MaxRecentMessages = 1
	cfg.CrossThreadSummaryCount = 2

	cm, err := NewPersistentContextManager(cfg, store)
	require.NoError(t, err)

	ctxA := axoncontext.WithThreadID(context.Background(), "thread-a")
	cm.AddMessages(ctxA,
		newTextMessage(RoleUser, "first"),
		newTextMessage(RoleAssistant, "second"),
	)
	_ = cm.Prepare(ctxA, cm.Messages(ctxA))

	ctxB := axoncontext.WithThreadID(context.Background(), "thread-b")
	cm.AddMessages(ctxB, newTextMessage(RoleUser, "hello"))
	res := cm.Prepare(ctxB, cm.Messages(ctxB))
	require.NotEmpty(t, res.Messages)
	assert.Equal(t, RoleSystem, res.Messages[0].Role)
	assert.Contains(t, res.Messages[0].Content.String(), "other threads")
}

func TestContextManagerDecorator_CanWrapAnotherStrategy(t *testing.T) {
	base := NewSimpleContextManager(nil)
	ctx := axoncontext.WithThreadID(context.Background(), "thread-x")
	base.AddMessages(ctx, newTextMessage(RoleUser, "one"), newTextMessage(RoleAssistant, "two"))

	innerCfg := DefaultContextManagerConfig()
	innerCfg.MaxRecentMessages = 1
	innerCfg.CrossThreadSummaryCount = 0
	inner, err := NewPersistentContextManagerWithNext(base, innerCfg, NewContextManagerMemoryStore())
	require.NoError(t, err)

	outerCfg := DefaultContextManagerConfig()
	outerCfg.MaxRecentMessages = 1
	outerCfg.CrossThreadSummaryCount = 0
	outer, err := NewPersistentContextManagerWithNext(inner, outerCfg, NewContextManagerMemoryStore())
	require.NoError(t, err)

	res := outer.Prepare(ctx, outer.Messages(ctx))
	require.True(t, res.Compacted)
	assert.NotEmpty(t, res.PrunedHistory)
}

func TestContextManagerRecordUsage_PersistsAcrossRestart(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "context", "state.json")
	store := NewContextManagerFileStore(statePath)

	cfg := DefaultContextManagerConfig()
	cm1, err := NewPersistentContextManager(cfg, store)
	require.NoError(t, err)

	cm1.RecordUsage(axoncontext.WithThreadID(context.Background(), "thread-a"), Usage{InputTokens: 10, OutputTokens: 4})
	cm1.RecordUsage(axoncontext.WithThreadID(context.Background(), "thread-b"), Usage{InputTokens: 7, OutputTokens: 3})

	cm2, err := NewPersistentContextManager(cfg, store)
	require.NoError(t, err)

	snapshot := cm2.Snapshot()
	assert.Equal(t, int64(17), snapshot.TotalInputTokens)
	assert.Equal(t, int64(7), snapshot.TotalOutputTokens)
	assert.Equal(t, int64(10), snapshot.Threads["thread-a"].TotalInputTokens)
	assert.Equal(t, int64(3), snapshot.Threads["thread-b"].TotalOutputTokens)
}

func newTextMessage(role Role, text string) Message {
	return Message{
		Role:    role,
		Content: &Content{Text: &text},
	}
}
