package consumer

import (
	"sync"
)

// OffsetCommitter wraps OffsetManager with a goroutine-safe start/stop lifecycle.
type OffsetCommitter struct {
	manager *OffsetManager
	done    chan struct{}
	once    sync.Once
}

// NewOffsetCommitter creates a new OffsetCommitter.
func NewOffsetCommitter(manager *OffsetManager) *OffsetCommitter {
	return &OffsetCommitter{
		manager: manager,
		done:    make(chan struct{}),
	}
}

// Start begins periodic offset commits.
func (c *OffsetCommitter) Start() {
	go c.manager.CommitLoop(c.done)
}

// Stop signals the commit loop to stop and performs a final commit.
func (c *OffsetCommitter) Stop() {
	c.once.Do(func() {
		close(c.done)
	})
}

// Commit performs an immediate offset commit.
func (c *OffsetCommitter) Commit() error {
	return c.manager.Commit()
}
