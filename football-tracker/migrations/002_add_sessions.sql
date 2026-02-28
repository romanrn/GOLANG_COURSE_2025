-- Add sessions table for session-based authentication
-- This table stores user session tokens with expiration

CREATE TABLE IF NOT EXISTS sessions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token VARCHAR(255) UNIQUE NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Index for fast token lookup
    CONSTRAINT idx_sessions_token UNIQUE (token)
);

-- Index for user_id to quickly find all sessions for a user
CREATE INDEX idx_sessions_user_id ON sessions(user_id);

-- Index for expires_at to efficiently clean up expired sessions
CREATE INDEX idx_sessions_expires_at ON sessions(expires_at);

-- Comment on table
COMMENT ON TABLE sessions IS 'User session tokens with expiration time';
COMMENT ON COLUMN sessions.user_id IS 'Foreign key to users table';
COMMENT ON COLUMN sessions.token IS 'Cryptographically secure random session token (64 hex characters)';
COMMENT ON COLUMN sessions.expires_at IS 'Session expiration timestamp';

