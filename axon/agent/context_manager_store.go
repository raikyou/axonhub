package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type ContextManagerStore interface {
	Load(ctx context.Context) (ContextManagerState, error)
	Save(ctx context.Context, state ContextManagerState) error
}

type ContextManagerMemoryStore struct {
	mu    sync.RWMutex
	state ContextManagerState
}

func NewContextManagerMemoryStore() *ContextManagerMemoryStore {
	return &ContextManagerMemoryStore{state: emptyContextState()}
}

func (s *ContextManagerMemoryStore) Load(_ context.Context) (ContextManagerState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return copyContextState(s.state), nil
}

func (s *ContextManagerMemoryStore) Save(_ context.Context, state ContextManagerState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = copyContextState(state)
	return nil
}

type ContextManagerFileStore struct {
	path string
	mu   sync.Mutex
}

func NewContextManagerFileStore(path string) *ContextManagerFileStore {
	return &ContextManagerFileStore{path: path}
}

func (s *ContextManagerFileStore) Load(_ context.Context) (ContextManagerState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return emptyContextState(), nil
		}
		return ContextManagerState{}, fmt.Errorf("read context manager state: %w", err)
	}

	var state ContextManagerState
	if err := json.Unmarshal(data, &state); err != nil {
		return ContextManagerState{}, fmt.Errorf("unmarshal context manager state: %w", err)
	}

	return normalizeContextState(state), nil
}

func (s *ContextManagerFileStore) Save(_ context.Context, state ContextManagerState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create context manager directory: %w", err)
	}

	state = normalizeContextState(state)
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal context manager state: %w", err)
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write context manager state temp file: %w", err)
	}

	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename context manager state file: %w", err)
	}

	return nil
}
