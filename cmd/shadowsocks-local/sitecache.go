package main

import (
	"sync"
	"time"
)

type site struct {
	sync.RWMutex
	host      string
	expire    time.Time
	confirmed bool
}

func (s *site) extend(duration time.Duration, confirm bool) bool {
	s.Lock()
	defer s.Unlock()

	cur := time.Now()
	t := cur.Add(duration)
	s.expire = t

	if s.confirmed != confirm {
		s.confirmed = confirm
		return true
	}

	return false
}

// with lock holded
func (s *site) _expired() bool {
	return time.Now().After(s.expire)
}

func (s *site) expired() bool {
	s.RLock()
	defer s.RUnlock()
	return time.Now().After(s.expire)
}

type siteCache struct {
	sync.RWMutex
	httpSites map[string]*site
}

func NewSiteCache() *siteCache {
	http := make(map[string]*site)
	return &siteCache{httpSites: http}
}

func (c *siteCache) Get(host string) (*site, bool) {
	c.RLock()
	defer c.RUnlock()

	s, ok := c.httpSites[host]
	if !ok {
		return nil, false
	}

	s.RLock()
	defer s.RUnlock()
	if s._expired() {
		return nil, false
	}

	return s, s.confirmed
}

func (c *siteCache) Add(host string) bool {
	c.Lock()
	defer c.Unlock()

	var s *site
	var ok bool
	if s, ok = c.httpSites[host]; ok {
		if s.expired() {
			s.extend(5*time.Minute, false)
			return true
		}
	} else {
		s = &site{host: host}
		s.extend(5*time.Minute, false)
		c.httpSites[host] = s
		return true
	}

	return false
}

func (c *siteCache) Confirm(host string) bool {
	c.RLock()
	defer c.RUnlock()

	if s, ok := c.httpSites[host]; ok {
		return s.extend(30*time.Minute, true)
	}

	return false
}
