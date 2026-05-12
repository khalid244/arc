package rollup

import "sync"

// Control holds the in-memory pause/rebuild flags for the builder. It's
// populated by HTTP handlers and read by the scheduler.
type Control struct {
	mu      sync.Mutex
	paused  map[string]bool
	rebuild map[string]bool
}

func NewControl() *Control {
	return &Control{
		paused:  map[string]bool{},
		rebuild: map[string]bool{},
	}
}

func (c *Control) Pause(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused[name] = true
}

func (c *Control) Resume(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.paused, name)
}

func (c *Control) IsPaused(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused[name]
}

func (c *Control) RequestRebuild(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rebuild[name] = true
}

// PopRebuildRequest returns true exactly once per RequestRebuild call.
func (c *Control) PopRebuildRequest(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rebuild[name] {
		delete(c.rebuild, name)
		return true
	}
	return false
}
