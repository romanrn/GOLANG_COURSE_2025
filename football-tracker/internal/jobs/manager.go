package jobs

import (
	"context"
	"football-tracker/cmd/server/logger"
	"log/slog"
	"sync"
)

// Manager manages the lifecycle of all background jobs
type Manager struct {
	jobs []Job
	wg   sync.WaitGroup
}

// NewManager creates a new job manager
func NewManager() *Manager {
	return &Manager{
		jobs: make([]Job, 0),
	}
}

// Register adds a job to the manager
func (m *Manager) Register(job Job) {
	m.jobs = append(m.jobs, job)
}

// StartAll starts all registered jobs in separate goroutines
func (m *Manager) StartAll(ctx context.Context) {
	if len(m.jobs) == 0 {
		logger.GetLogger().Info(ctx, "No background jobs to start")
		return
	}

	for _, job := range m.jobs {
		jobName := job.GetName()
		m.wg.Add(1)

		go func(j Job, name string) {
			defer m.wg.Done()
			j.Start(ctx)

			logger.GetLogger().Info(ctx, "Background job finished",
				slog.String("job", name))
		}(job, jobName)

		logger.GetLogger().Info(ctx, "Background job started",
			slog.String("job", jobName))
	}

	logger.GetLogger().Info(ctx, "All background jobs started",
		slog.Int("count", len(m.jobs)))
}

// StopAll gracefully stops all registered jobs and waits for them to finish
func (m *Manager) StopAll(ctx context.Context) {
	if len(m.jobs) == 0 {
		logger.GetLogger().Info(ctx, "No background jobs to stop")
		return
	}

	logger.GetLogger().Info(ctx, "Stopping all background jobs...",
		slog.Int("count", len(m.jobs)))

	// Send stop signal to all jobs
	for _, job := range m.jobs {
		job.Stop()
	}

	// Wait for all jobs to finish
	m.wg.Wait()

	logger.GetLogger().Info(ctx, "All background jobs stopped",
		slog.Int("count", len(m.jobs)))
}
