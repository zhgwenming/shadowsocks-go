package main

import (
	"sync"
	"time"
)

type site struct {
	sync.RWMutex
	host   string
	expire time.Time
}

func (s *site) extend(duration time.Duration) {
	s.Lock()
	defer s.Unlock()

	cur := time.Now()
	t := cur.Add(duration)
	s.expire = t
}

func (s *site) expired() bool {
	s.RLock()
	defer s.RUnlock()
	if time.Now().Before(s.expire) {
		return false
	}

	return true
}

type siteCache struct {
	sync.RWMutex
	httpSites map[string]*site
}

func NewSiteCache() *siteCache {
	http := make(map[string]*site)
	return &siteCache{httpSites: http}
}

func (c *siteCache) Get(host string) *site {
	c.RLock()
	defer c.RUnlock()

	var s *site
	if s, ok := c.httpSites[host]; ok {
		if s.expired() {
			return nil
		}
	}

	return s
}

func (c *siteCache) Add(host string) bool {
	c.Lock()
	defer c.Unlock()

	var s *site
	var ok bool
	if s, ok = c.httpSites[host]; ok {
		if s.expired() {
			s.extend(5 * time.Minute)
			return true
		}
	} else {
		s = &site{host: host}
		s.extend(5 * time.Minute)
		c.httpSites[host] = s
		return true
	}

	return false
}

func (c *siteCache) Confirm(host string) bool {
	c.Lock()
	defer c.Unlock()

	if s, ok := c.httpSites[host]; ok {
		s.extend(7 * 24 * time.Hour)
		return true
	}
	return false
}
