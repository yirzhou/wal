package main

import (
	"errors"
	"fmt"
	"sync"
)

type MemState struct {
	mu    sync.Mutex
	state map[string][]byte // key -> value
}

func NewMemState() *MemState {
	return &MemState{
		state: make(map[string][]byte),
	}
}

func (m *MemState) Get(key []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	value, ok := m.state[string(key)]
	if !ok {
		return nil, errors.New("key not found")
	}
	return value, nil
}

func (m *MemState) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state[string(key)] = value
	return nil
}

func (m *MemState) Print() {
	fmt.Println("========== MemState starts ==========")
	for k, v := range m.state {
		fmt.Printf("%s: %s\n", k, string(v))
	}
	fmt.Println("========== MemState ends ==========")
}
