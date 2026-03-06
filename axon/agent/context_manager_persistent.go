package agent

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	axoncontext "github.com/looplj/axonhub/axon/context"
)

// ContextManagerConfig controls context compaction and cross-thread memory behavior.
type ContextManagerConfig struct {
	Enabled                 bool
	MaxRecentMessages       int
	SoftTokenLimit          int
	CrossThreadSummaryCount int
	SummaryMaxChars         int
}

func DefaultContextManagerConfig() ContextManagerConfig {
	return ContextManagerConfig{
		Enabled:                 true,
		MaxRecentMessages:       defaultContextMaxRecentMessages,
		SoftTokenLimit:          defaultContextSoftTokenLimit,
		CrossThreadSummaryCount: defaultContextCrossThreadSummaryNum,
		SummaryMaxChars:         defaultContextSummaryMaxChars,
	}
}

// PersistentContextManager is a decorator strategy that adds compaction,
// cross-thread summaries, and persisted token accounting.
type PersistentContextManager struct {
	ContextManager

	config ContextManagerConfig
	store  ContextManagerStore

	mu    sync.RWMutex
	state ContextManagerState
}

func NewPersistentContextManager(config ContextManagerConfig, store ContextManagerStore) (*PersistentContextManager, error) {
	return NewPersistentContextManagerWithNext(nil, config, store)
}

func NewPersistentContextManagerWithNext(next ContextManager, config ContextManagerConfig, store ContextManagerStore) (*PersistentContextManager, error) {
	cfg := normalizeContextManagerConfig(config)
	if next == nil {
		next = NewSimpleContextManager(nil)
	}

	cm := &PersistentContextManager{
		ContextManager: next,
		config:         cfg,
		store:          store,
		state:          emptyContextState(),
	}

	if store == nil {
		return cm, nil
	}

	loaded, err := store.Load(context.Background())
	if err != nil {
		return nil, err
	}
	cm.state = normalizeContextState(loaded)
	return cm, nil
}

func (m *PersistentContextManager) Prepare(ctx context.Context, history []Message) ContextPrepareResult {
	downstream := m.ContextManager.Prepare(ctx, history)
	working := cloneMessages(downstream.Messages)
	compacted := downstream.Compacted
	pruned := cloneMessages(downstream.PrunedHistory)

	if !m.config.Enabled {
		downstream.Messages = working
		downstream.Compacted = compacted
		downstream.PrunedHistory = pruned
		return downstream
	}

	threadID := axoncontext.ThreadID(ctx)

	m.mu.Lock()
	defer m.mu.Unlock()

	threadState := m.ensureThreadLocked(threadID)
	keep := m.config.MaxRecentMessages
	if keep <= 0 {
		keep = defaultContextMaxRecentMessages
	}

	if len(working) > keep || (m.config.SoftTokenLimit > 0 && EstimateMessagesTokens(working) > m.config.SoftTokenLimit && len(working) > keep/2) {
		if keep > len(working) {
			keep = len(working)
		}

		overflow := working[:len(working)-keep]
		if len(overflow) > 0 {
			piece := summarizeMessages(overflow)
			if piece != "" {
				threadState.Summary = mergeSummary(threadState.Summary, piece, m.config.SummaryMaxChars)
				threadState.CompactionCount++
				threadState.LastUpdatedAt = time.Now().UTC()
				m.state.Threads[threadID] = threadState
				m.state.UpdatedAt = threadState.LastUpdatedAt
				m.touchThreadOrderLocked(threadID)
				m.saveLocked(ctx)
			}

			working = cloneMessages(working[len(working)-keep:])
			compacted = true
			pruned = cloneMessages(working)
		}
	}

	promptMessages := m.prependSummariesLocked(threadID, working)
	return ContextPrepareResult{
		Messages:      promptMessages,
		Compacted:     compacted,
		PrunedHistory: pruned,
	}
}

func (m *PersistentContextManager) RecordUsage(ctx context.Context, usage Usage) {
	m.ContextManager.RecordUsage(ctx, usage)

	if !m.config.Enabled {
		return
	}

	threadID := axoncontext.ThreadID(ctx)

	m.mu.Lock()
	defer m.mu.Unlock()

	threadState := m.ensureThreadLocked(threadID)
	threadState.TotalInputTokens += int64(usage.InputTokens)
	threadState.TotalOutputTokens += int64(usage.OutputTokens)
	threadState.LastUpdatedAt = time.Now().UTC()

	m.state.Threads[threadID] = threadState
	m.state.TotalInputTokens += int64(usage.InputTokens)
	m.state.TotalOutputTokens += int64(usage.OutputTokens)
	m.state.UpdatedAt = threadState.LastUpdatedAt
	m.touchThreadOrderLocked(threadID)
	m.saveLocked(ctx)
}

func (m *PersistentContextManager) Snapshot() ContextManagerState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return copyContextState(m.state)
}

func (m *PersistentContextManager) prependSummariesLocked(threadID string, history []Message) []Message {
	output := make([]Message, 0, len(history)+2)

	if ts, ok := m.state.Threads[threadID]; ok && strings.TrimSpace(ts.Summary) != "" {
		summary := fmt.Sprintf("Summary of earlier conversation in this thread:\n%s", ts.Summary)
		output = append(output, Message{Role: RoleSystem, Content: &Content{Text: &summary}})
	}

	if cross := m.buildCrossThreadSummaryLocked(threadID); cross != "" {
		text := fmt.Sprintf("Relevant summaries from other threads:\n%s", cross)
		output = append(output, Message{Role: RoleSystem, Content: &Content{Text: &text}})
	}

	output = append(output, history...)
	return output
}

func (m *PersistentContextManager) buildCrossThreadSummaryLocked(currentThread string) string {
	limit := m.config.CrossThreadSummaryCount
	if limit <= 0 {
		return ""
	}

	parts := make([]string, 0, limit)
	seen := make(map[string]struct{})

	for i := len(m.state.ThreadOrder) - 1; i >= 0; i-- {
		id := m.state.ThreadOrder[i]
		if id == currentThread {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}

		ts, ok := m.state.Threads[id]
		if !ok || strings.TrimSpace(ts.Summary) == "" {
			continue
		}

		parts = append(parts, fmt.Sprintf("- [%s] %s", id, ts.Summary))
		if len(parts) >= limit {
			break
		}
	}

	if len(parts) == 0 {
		return ""
	}

	return strings.Join(parts, "\n")
}

func (m *PersistentContextManager) ensureThreadLocked(threadID string) ContextThreadState {
	state := normalizeContextState(m.state)
	m.state = state

	ts, ok := m.state.Threads[threadID]
	if !ok {
		now := time.Now().UTC()
		ts = ContextThreadState{
			ThreadID:      threadID,
			LastUpdatedAt: now,
		}
		m.state.Threads[threadID] = ts
		m.state.UpdatedAt = now
	}
	return ts
}

func (m *PersistentContextManager) touchThreadOrderLocked(threadID string) {
	if threadID == "" {
		return
	}
	for i := len(m.state.ThreadOrder) - 1; i >= 0; i-- {
		if m.state.ThreadOrder[i] != threadID {
			continue
		}
		m.state.ThreadOrder = append(m.state.ThreadOrder[:i], m.state.ThreadOrder[i+1:]...)
		break
	}
	m.state.ThreadOrder = append(m.state.ThreadOrder, threadID)
}

func (m *PersistentContextManager) saveLocked(ctx context.Context) {
	if m.store == nil {
		return
	}
	_ = m.store.Save(ctx, m.state)
}

func normalizeContextManagerConfig(cfg ContextManagerConfig) ContextManagerConfig {
	if cfg.MaxRecentMessages <= 0 {
		cfg.MaxRecentMessages = defaultContextMaxRecentMessages
	}
	if cfg.SoftTokenLimit <= 0 {
		cfg.SoftTokenLimit = defaultContextSoftTokenLimit
	}
	if cfg.CrossThreadSummaryCount < 0 {
		cfg.CrossThreadSummaryCount = 0
	}
	if cfg.CrossThreadSummaryCount == 0 {
		cfg.CrossThreadSummaryCount = defaultContextCrossThreadSummaryNum
	}
	if cfg.SummaryMaxChars <= 0 {
		cfg.SummaryMaxChars = defaultContextSummaryMaxChars
	}
	return cfg
}
