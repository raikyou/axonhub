package agent

import (
	"context"
	"sync"

	axoncontext "github.com/looplj/axonhub/axon/context"
)

// SimpleContextManager keeps in-memory history and preserves current behavior.
type SimpleContextManager struct {
	mu      sync.RWMutex
	threads map[string][]Message
}

func NewSimpleContextManager(initial []Message) *SimpleContextManager {
	m := &SimpleContextManager{threads: make(map[string][]Message)}
	if len(initial) > 0 {
		m.threads[axoncontext.ThreadID(context.Background())] = cloneMessages(initial)
	}
	return m
}

func (m *SimpleContextManager) AddMessages(ctx context.Context, msgs ...Message) {
	if len(msgs) == 0 {
		return
	}
	threadID := axoncontext.ThreadID(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.threads[threadID] = append(m.threads[threadID], msgs...)
}

func (m *SimpleContextManager) Messages(ctx context.Context) []Message {
	threadID := axoncontext.ThreadID(ctx)
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cloneMessages(m.threads[threadID])
}

func (m *SimpleContextManager) ReplaceMessages(ctx context.Context, msgs []Message) {
	threadID := axoncontext.ThreadID(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.threads[threadID] = cloneMessages(msgs)
}

func (m *SimpleContextManager) ClearMessages(ctx context.Context) {
	threadID := axoncontext.ThreadID(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.threads, threadID)
}

func (m *SimpleContextManager) Prepare(_ context.Context, history []Message) ContextPrepareResult {
	return ContextPrepareResult{Messages: cloneMessages(history)}
}

func (m *SimpleContextManager) RecordUsage(_ context.Context, _ Usage) {}

func (m *SimpleContextManager) Snapshot() ContextManagerState {
	return emptyContextState()
}

var _ ContextManager = (*SimpleContextManager)(nil)
