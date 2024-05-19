package kvstore

import "sync"

type Set struct {
	Map map[string]bool
	sync.RWMutex
}

func (s *Set) Add(values ...string) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	for _, v := range values {
		s.Map[v] = true
	}
}
