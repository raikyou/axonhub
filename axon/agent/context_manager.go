package agent

import (
	"context"
	"time"
)

const (
	defaultContextMaxRecentMessages     = 80
	defaultContextSoftTokenLimit        = 120000
	defaultContextCrossThreadSummaryNum = 2
	defaultContextSummaryMaxChars       = 16000
)

type ContextPrepareResult struct {
	Messages      []Message
	PrunedHistory []Message
	Compacted     bool
}

// ContextManager manages message history and optional context preparation policies.
// Implementations can be composed via decorators.
type ContextManager interface {
	AddMessages(ctx context.Context, msgs ...Message)
	Messages(ctx context.Context) []Message
	ReplaceMessages(ctx context.Context, msgs []Message)
	ClearMessages(ctx context.Context)

	Prepare(ctx context.Context, history []Message) ContextPrepareResult
	RecordUsage(ctx context.Context, usage Usage)
	Snapshot() ContextManagerState
}

type ContextManagerState struct {
	Threads           map[string]ContextThreadState `json:"threads"`
	ThreadOrder       []string                      `json:"thread_order,omitempty"`
	TotalInputTokens  int64                         `json:"total_input_tokens"`
	TotalOutputTokens int64                         `json:"total_output_tokens"`
	UpdatedAt         time.Time                     `json:"updated_at"`
}

type ContextThreadState struct {
	ThreadID          string    `json:"thread_id"`
	Summary           string    `json:"summary,omitempty"`
	CompactionCount   int64     `json:"compaction_count"`
	TotalInputTokens  int64     `json:"total_input_tokens"`
	TotalOutputTokens int64     `json:"total_output_tokens"`
	LastUpdatedAt     time.Time `json:"last_updated_at"`
}

func emptyContextState() ContextManagerState {
	return ContextManagerState{
		Threads: make(map[string]ContextThreadState),
	}
}

func normalizeContextState(state ContextManagerState) ContextManagerState {
	if state.Threads == nil {
		state.Threads = make(map[string]ContextThreadState)
	}
	if state.ThreadOrder == nil {
		state.ThreadOrder = make([]string, 0)
	}
	return state
}

func copyContextState(state ContextManagerState) ContextManagerState {
	state = normalizeContextState(state)
	out := ContextManagerState{
		Threads:           make(map[string]ContextThreadState, len(state.Threads)),
		ThreadOrder:       append([]string(nil), state.ThreadOrder...),
		TotalInputTokens:  state.TotalInputTokens,
		TotalOutputTokens: state.TotalOutputTokens,
		UpdatedAt:         state.UpdatedAt,
	}
	for k, v := range state.Threads {
		out.Threads[k] = v
	}
	return out
}


