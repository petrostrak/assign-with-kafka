package main

import (
	"fmt"
	"sync"
)

type Storer interface {
	Put(MessageState, []byte) error
	Get(state MessageState) ([]byte, error)
}

type Storage struct {
	data map[MessageState][]byte
	mu   sync.RWMutex
}

func NewStorage() *Storage {
	return &Storage{
		data: make(map[MessageState][]byte),
	}
}

func (s *Storage) Put(state MessageState, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[state] = value

	return nil
}

func (s *Storage) Get(state MessageState) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[state]
	if !ok {
		return nil, fmt.Errorf("value not found")
	}
	return value, nil
}
