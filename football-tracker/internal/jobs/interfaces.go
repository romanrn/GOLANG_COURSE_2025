package jobs

import (
	"context"
	"time"
)

// Job represents a background task that runs periodically
type Job interface {
	// Start begins the job execution in a blocking manner
	// Should be called with `go job.Start(ctx)` to run in background
	Start(ctx context.Context)

	// Stop gracefully stops the job
	// Blocks until the job has fully stopped
	Stop()

	// GetName returns the job name for logging
	GetName() string
}

// CleanupJob represents a job that performs cleanup operations
type CleanupJob interface {
	Job

	// GetInterval returns the cleanup interval
	GetInterval() time.Duration
}
