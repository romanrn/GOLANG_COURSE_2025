-- Create championships table
CREATE TABLE IF NOT EXISTS championships (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    year INTEGER NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    logo_url VARCHAR(500),  -- URL to championship logo
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Also used for optimistic locking
);

-- Create host_countries table
CREATE TABLE IF NOT EXISTS host_countries (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    code CHAR(3) NOT NULL UNIQUE,  -- ISO 3166-1 alpha-3: USA, MEX, CAN, etc.
    flag_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create championship_hosts junction table (many-to-many)
CREATE TABLE IF NOT EXISTS championship_hosts (
    championship_id INTEGER REFERENCES championships(id) ON DELETE CASCADE,
    host_country_id INTEGER REFERENCES host_countries(id) ON DELETE CASCADE,
    PRIMARY KEY (championship_id, host_country_id)
);

-- Create cities table
CREATE TABLE IF NOT EXISTS cities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country_code CHAR(3) NOT NULL,  -- Country Code  USA, MEX, CAN
    stadium VARCHAR(255),            -- Main stadium
    timezone VARCHAR(50),            -- Time Zone: America/New_York
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, country_code)
);

-- Create championship_cities junction table
CREATE TABLE IF NOT EXISTS championship_cities (
    championship_id INTEGER REFERENCES championships(id) ON DELETE CASCADE,
    city_id INTEGER REFERENCES cities(id) ON DELETE CASCADE,
    PRIMARY KEY (championship_id, city_id)
);

-- Create teams table
CREATE TABLE IF NOT EXISTS teams (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,         -- Team name (e.g., "Argentina National Team")
    country_code CHAR(3) NOT NULL,      -- ISO 3166-1 alpha-3: ARG, BRA, GER, etc.
    country_name VARCHAR(100) NOT NULL, -- Full country name for display
    flag_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, country_code)  -- Team name unique per country
);

-- Create championship_teams junction table (many-to-many)
CREATE TABLE IF NOT EXISTS championship_teams (
    championship_id INTEGER REFERENCES championships(id) ON DELETE CASCADE,
    team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
    PRIMARY KEY (championship_id, team_id)
);

-- Create groups table
CREATE TABLE IF NOT EXISTS groups (
    id SERIAL PRIMARY KEY,
    name VARCHAR(10) NOT NULL,
    championship_id INTEGER NOT NULL REFERENCES championships(id) ON DELETE CASCADE,
    UNIQUE(name, championship_id)
);

-- Create team_groups table
CREATE TABLE IF NOT EXISTS team_groups (
    team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    championship_id INTEGER NOT NULL REFERENCES championships(id) ON DELETE CASCADE,
    PRIMARY KEY (team_id, championship_id),
    UNIQUE (team_id, group_id)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_team_groups_team ON team_groups(team_id);
CREATE INDEX IF NOT EXISTS idx_team_groups_group ON team_groups(group_id);
CREATE INDEX IF NOT EXISTS idx_team_groups_championship ON team_groups(championship_id);

-- Create matches table
CREATE TABLE IF NOT EXISTS matches (
    id SERIAL PRIMARY KEY,
    championship_id INTEGER REFERENCES championships(id) ON DELETE CASCADE,
    group_id INTEGER REFERENCES groups(id) ON DELETE SET NULL,
    home_team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
    away_team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
    city_id INTEGER REFERENCES cities(id) ON DELETE SET NULL,  -- City where match is played
    match_date TIMESTAMP NOT NULL,

    -- Score breakdown for different periods
    home_score_regular INTEGER,      -- Score in regular time (90 minutes)
    away_score_regular INTEGER,      -- Score in regular time (90 minutes)

    home_score_total INTEGER,        -- Total score (regular + extra), does NOT include penalties
    away_score_total INTEGER,        -- Total score (regular + extra), does NOT include penalties

    -- Flags to indicate match progression
    has_extra_time BOOLEAN DEFAULT FALSE,  -- Whether match went to extra time
    has_penalty BOOLEAN DEFAULT FALSE,     -- Whether match went to penalty shootout

    status CHAR(2) NOT NULL,  -- Short code: SC, LV, FN, CN, PP
    stage CHAR(2) NOT NULL,   -- Short code: GR, 16, 32, QF, SF, FN, TP
    venue VARCHAR(255),       -- Specific stadium name (deprecated, use city.stadium instead)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints to ensure data consistency
    CONSTRAINT check_scores_non_negative CHECK (
        (home_score_regular IS NULL OR home_score_regular >= 0) AND
        (away_score_regular IS NULL OR away_score_regular >= 0) AND
        (home_score_total IS NULL OR home_score_total >= 0) AND
        (away_score_total IS NULL OR away_score_total >= 0)
    ),
    CONSTRAINT check_total_score_consistency CHECK (
        (home_score_total IS NULL AND away_score_total IS NULL) OR
        (home_score_total IS NOT NULL AND away_score_total IS NOT NULL)
    )
);

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password CHAR(60) NOT NULL,  -- Bcrypt hash is always 60 characters
    role CHAR(2) NOT NULL,       -- AD=admin, US=user (short codes for efficiency)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- NOTE: password field stores bcrypt hash, not plain text
-- Bcrypt format: $2a$10$[22 chars salt][31 chars hash] = 60 chars total
-- role: AD=admin (can edit all data), US=user (can only manage own predictions)

-- Create predictions table
CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    match_id INTEGER REFERENCES matches(id) ON DELETE CASCADE,

    -- Regular time prediction (required for all matches)
    home_score_regular INTEGER NOT NULL,
    away_score_regular INTEGER NOT NULL,

    -- Total score prediction (optional, for playoff matches)
    home_score_total INTEGER,         -- Predicted total score (regular + extra time)
    away_score_total INTEGER,         -- Predicted total score (regular + extra time)

    -- Penalty shootout winner prediction (optional, for playoff matches)
    penalty_winner_team_id INTEGER REFERENCES teams(id) ON DELETE SET NULL,

    points INTEGER,                    -- Calculated points based on prediction accuracy
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, match_id),

    -- Constraints
    CONSTRAINT check_scores_non_negative CHECK (
        home_score_regular >= 0 AND away_score_regular >= 0
    ),
    CONSTRAINT check_total_scores_consistency CHECK (
        (home_score_total IS NULL AND away_score_total IS NULL) OR
        (home_score_total IS NOT NULL AND away_score_total IS NOT NULL AND
         home_score_total >= 0 AND away_score_total >= 0)
    )
);

-- Create championship_user_ratings table
CREATE TABLE IF NOT EXISTS championship_user_ratings (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    championship_id INTEGER NOT NULL REFERENCES championships(id) ON DELETE CASCADE,
    total_points INTEGER DEFAULT 0,
    total_predictions INTEGER DEFAULT 0,
    correct_scores INTEGER DEFAULT 0,
    correct_outcomes INTEGER DEFAULT 0,
    wrong_predictions INTEGER DEFAULT 0,
    accuracy_percentage DECIMAL(5,2) DEFAULT 0.00,
    average_points DECIMAL(5,2) DEFAULT 0.00,
    rank INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, championship_id)
);

-- Create user_ratings table
CREATE TABLE IF NOT EXISTS user_ratings (
    user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    total_points INTEGER DEFAULT 0,
    total_predictions INTEGER DEFAULT 0,
    total_championships INTEGER DEFAULT 0,
    correct_scores INTEGER DEFAULT 0,
    correct_outcomes INTEGER DEFAULT 0,
    wrong_predictions INTEGER DEFAULT 0,
    accuracy_percentage DECIMAL(5,2) DEFAULT 0.00,
    average_points DECIMAL(5,2) DEFAULT 0.00,
    global_rank INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for fast ranking queries
CREATE INDEX IF NOT EXISTS idx_championship_user_ratings_championship ON championship_user_ratings(championship_id);
CREATE INDEX IF NOT EXISTS idx_championship_user_ratings_user ON championship_user_ratings(user_id);
CREATE INDEX IF NOT EXISTS idx_championship_user_ratings_points ON championship_user_ratings(total_points DESC);
CREATE INDEX IF NOT EXISTS idx_championship_user_ratings_rank ON championship_user_ratings(rank);
CREATE INDEX IF NOT EXISTS idx_user_ratings_points ON user_ratings(total_points DESC);
CREATE INDEX IF NOT EXISTS idx_user_ratings_global_rank ON user_ratings(global_rank);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date);
CREATE INDEX IF NOT EXISTS idx_matches_championship ON matches(championship_id);
CREATE INDEX IF NOT EXISTS idx_matches_status ON matches(status);
CREATE INDEX IF NOT EXISTS idx_matches_city ON matches(city_id);
CREATE INDEX IF NOT EXISTS idx_predictions_user ON predictions(user_id);
CREATE INDEX IF NOT EXISTS idx_predictions_match ON predictions(match_id);
CREATE INDEX IF NOT EXISTS idx_predictions_points ON predictions(points);
CREATE INDEX IF NOT EXISTS idx_championship_teams_championship ON championship_teams(championship_id);
CREATE INDEX IF NOT EXISTS idx_championship_teams_team ON championship_teams(team_id);
CREATE INDEX IF NOT EXISTS idx_groups_championship ON groups(championship_id);
CREATE INDEX IF NOT EXISTS idx_championship_hosts_championship ON championship_hosts(championship_id);
CREATE INDEX IF NOT EXISTS idx_championship_hosts_country ON championship_hosts(host_country_id);
CREATE INDEX IF NOT EXISTS idx_championship_cities_championship ON championship_cities(championship_id);
CREATE INDEX IF NOT EXISTS idx_championship_cities_city ON championship_cities(city_id);

-- NOTE: All business logic (updating updated_at, calculating ratings) is handled in the application layer
-- No triggers - keeps logic transparent, testable, and maintainable

-- Insert sample data for World Cup 2026
INSERT INTO championships (name, year, start_date, end_date, logo_url)
VALUES ('FIFA World Cup', 2026, '2026-06-11', '2026-07-19', 'https://upload.wikimedia.org/wikipedia/en/8/86/2026_FIFA_World_Cup.svg')
ON CONFLICT DO NOTHING;

-- Insert host countries for World Cup 2026
INSERT INTO host_countries (name, code, flag_url) VALUES
    ('United States', 'USA', 'https://flagcdn.com/w320/us.png'),
    ('Mexico', 'MEX', 'https://flagcdn.com/w320/mx.png'),
    ('Canada', 'CAN', 'https://flagcdn.com/w320/ca.png')
ON CONFLICT (code) DO NOTHING;

-- Insert cities for World Cup 2026
INSERT INTO cities (name, country_code, stadium, timezone) VALUES
    ('New York', 'USA', 'MetLife Stadium', 'America/New_York'),
    ('Los Angeles', 'USA', 'Rose Bowl', 'America/Los_Angeles'),
    ('Dallas', 'USA', 'AT&T Stadium', 'America/Chicago'),
    ('Philadelphia', 'USA', 'Lincoln Financial Field', 'America/New_York'),
    ('Atlanta', 'USA', 'Mercedes-Benz Stadium', 'America/New_York'),
    ('Kansas City', 'USA', 'Arrowhead Stadium', 'America/Chicago'),
    ('Santa Clara', 'USA', 'Levi''s Stadium', 'America/Los_Angeles'),
    ('Mexico City', 'MEX', 'Estadio Azteca', 'America/Mexico_City'),
    ('Toronto', 'CAN', 'BMO Field', 'America/Toronto')
ON CONFLICT (name, country_code) DO NOTHING;

-- Get the championship ID
DO $$
DECLARE
    championship_id_var INTEGER;
    host_usa_id INTEGER;
    host_mex_id INTEGER;
    host_can_id INTEGER;
    city_ny_id INTEGER;
    city_la_id INTEGER;
    city_dallas_id INTEGER;
    city_philly_id INTEGER;
    city_atlanta_id INTEGER;
    city_kc_id INTEGER;
    city_sc_id INTEGER;
BEGIN
    SELECT id INTO championship_id_var FROM championships WHERE year = 2026 AND name = 'FIFA World Cup';

    -- Get host country IDs
    SELECT id INTO host_usa_id FROM host_countries WHERE code = 'USA';
    SELECT id INTO host_mex_id FROM host_countries WHERE code = 'MEX';
    SELECT id INTO host_can_id FROM host_countries WHERE code = 'CAN';

    -- Link host countries to championship
    INSERT INTO championship_hosts (championship_id, host_country_id) VALUES
        (championship_id_var, host_usa_id),
        (championship_id_var, host_mex_id),
        (championship_id_var, host_can_id)
    ON CONFLICT DO NOTHING;

    -- Get city IDs
    SELECT id INTO city_ny_id FROM cities WHERE name = 'New York' AND country_code = 'USA';
    SELECT id INTO city_la_id FROM cities WHERE name = 'Los Angeles' AND country_code = 'USA';
    SELECT id INTO city_dallas_id FROM cities WHERE name = 'Dallas' AND country_code = 'USA';
    SELECT id INTO city_philly_id FROM cities WHERE name = 'Philadelphia' AND country_code = 'USA';
    SELECT id INTO city_atlanta_id FROM cities WHERE name = 'Atlanta' AND country_code = 'USA';
    SELECT id INTO city_kc_id FROM cities WHERE name = 'Kansas City' AND country_code = 'USA';
    SELECT id INTO city_sc_id FROM cities WHERE name = 'Santa Clara' AND country_code = 'USA';

    -- Link cities to championship
    INSERT INTO championship_cities (championship_id, city_id) VALUES
        (championship_id_var, city_ny_id),
        (championship_id_var, city_la_id),
        (championship_id_var, city_dallas_id),
        (championship_id_var, city_philly_id),
        (championship_id_var, city_atlanta_id),
        (championship_id_var, city_kc_id),
        (championship_id_var, city_sc_id)
    ON CONFLICT DO NOTHING;

    -- Insert groups A-H for World Cup 2026
    INSERT INTO groups (name, championship_id) VALUES
        ('A', championship_id_var),
        ('B', championship_id_var),
        ('C', championship_id_var),
        ('D', championship_id_var),
        ('E', championship_id_var),
        ('F', championship_id_var),
        ('G', championship_id_var),
        ('H', championship_id_var)
    ON CONFLICT DO NOTHING;

    -- Insert sample teams (you can add more teams later)
    INSERT INTO teams (name, country_code, country_name, flag_url) VALUES
        ('Argentina National Team', 'ARG', 'Argentina', 'https://flagcdn.com/w320/ar.png'),
        ('Brazil National Team', 'BRA', 'Brazil', 'https://flagcdn.com/w320/br.png'),
        ('Germany National Team', 'GER', 'Germany', 'https://flagcdn.com/w320/de.png'),
        ('France National Team', 'FRA', 'France', 'https://flagcdn.com/w320/fr.png'),
        ('Spain National Team', 'ESP', 'Spain', 'https://flagcdn.com/w320/es.png'),
        ('England National Team', 'ENG', 'England', 'https://flagcdn.com/w320/gb-eng.png'),
        ('Portugal National Team', 'POR', 'Portugal', 'https://flagcdn.com/w320/pt.png'),
        ('Netherlands National Team', 'NED', 'Netherlands', 'https://flagcdn.com/w320/nl.png'),
        ('Italy National Team', 'ITA', 'Italy', 'https://flagcdn.com/w320/it.png'),
        ('Belgium National Team', 'BEL', 'Belgium', 'https://flagcdn.com/w320/be.png'),
        ('Croatia National Team', 'HRV', 'Croatia', 'https://flagcdn.com/w320/hr.png'),
        ('Uruguay National Team', 'URY', 'Uruguay', 'https://flagcdn.com/w320/uy.png'),
        ('USA National Team', 'USA', 'United States', 'https://flagcdn.com/w320/us.png'),
        ('Mexico National Team', 'MEX', 'Mexico', 'https://flagcdn.com/w320/mx.png'),
        ('Canada National Team', 'CAN', 'Canada', 'https://flagcdn.com/w320/ca.png'),
        ('Japan National Team', 'JPN', 'Japan', 'https://flagcdn.com/w320/jp.png')
    ON CONFLICT (name, country_code) DO NOTHING;

    -- Assign teams to groups (sample distribution)
    DECLARE
        group_a_id INTEGER;
        group_b_id INTEGER;
        group_c_id INTEGER;
        group_d_id INTEGER;
        team_arg_id INTEGER;
        team_bra_id INTEGER;
        team_ger_id INTEGER;
        team_fra_id INTEGER;
        team_esp_id INTEGER;
        team_eng_id INTEGER;
        team_por_id INTEGER;
        team_ned_id INTEGER;
        team_ita_id INTEGER;
        team_bel_id INTEGER;
        team_cro_id INTEGER;
        team_uru_id INTEGER;
        team_usa_id INTEGER;
        team_mex_id INTEGER;
        team_can_id INTEGER;
        team_jpn_id INTEGER;
    BEGIN
        SELECT id INTO group_a_id FROM groups WHERE name = 'A' AND championship_id = championship_id_var;
        SELECT id INTO group_b_id FROM groups WHERE name = 'B' AND championship_id = championship_id_var;
        SELECT id INTO group_c_id FROM groups WHERE name = 'C' AND championship_id = championship_id_var;
        SELECT id INTO group_d_id FROM groups WHERE name = 'D' AND championship_id = championship_id_var;

        SELECT id INTO team_arg_id FROM teams WHERE country_name = 'Argentina';
        SELECT id INTO team_bra_id FROM teams WHERE country_name = 'Brazil';
        SELECT id INTO team_ger_id FROM teams WHERE country_name = 'Germany';
        SELECT id INTO team_fra_id FROM teams WHERE country_name = 'France';
        SELECT id INTO team_esp_id FROM teams WHERE country_name = 'Spain';
        SELECT id INTO team_eng_id FROM teams WHERE country_name = 'England';
        SELECT id INTO team_por_id FROM teams WHERE country_name = 'Portugal';
        SELECT id INTO team_ned_id FROM teams WHERE country_name = 'Netherlands';
        SELECT id INTO team_ita_id FROM teams WHERE country_name = 'Italy';
        SELECT id INTO team_bel_id FROM teams WHERE country_name = 'Belgium';
        SELECT id INTO team_cro_id FROM teams WHERE country_name = 'Croatia';
        SELECT id INTO team_uru_id FROM teams WHERE country_name = 'Uruguay';
        SELECT id INTO team_usa_id FROM teams WHERE country_name = 'United States';
        SELECT id INTO team_mex_id FROM teams WHERE country_name = 'Mexico';
        SELECT id INTO team_can_id FROM teams WHERE country_name = 'Canada';
        SELECT id INTO team_jpn_id FROM teams WHERE country_name = 'Japan';

        -- Link teams to championship
        INSERT INTO championship_teams (championship_id, team_id) VALUES
            (championship_id_var, team_arg_id),
            (championship_id_var, team_bra_id),
            (championship_id_var, team_ger_id),
            (championship_id_var, team_fra_id),
            (championship_id_var, team_esp_id),
            (championship_id_var, team_eng_id),
            (championship_id_var, team_por_id),
            (championship_id_var, team_ned_id),
            (championship_id_var, team_ita_id),
            (championship_id_var, team_bel_id),
            (championship_id_var, team_cro_id),
            (championship_id_var, team_uru_id),
            (championship_id_var, team_usa_id),
            (championship_id_var, team_mex_id),
            (championship_id_var, team_can_id),
            (championship_id_var, team_jpn_id)
        ON CONFLICT DO NOTHING;

        -- Assign teams to groups (sample distribution)
        INSERT INTO team_groups (team_id, group_id, championship_id) VALUES
            (team_arg_id, group_a_id, championship_id_var),
            (team_usa_id, group_a_id, championship_id_var),
            (team_mex_id, group_a_id, championship_id_var),
            (team_can_id, group_a_id, championship_id_var),
            (team_bra_id, group_b_id, championship_id_var),
            (team_ger_id, group_b_id, championship_id_var),
            (team_jpn_id, group_b_id, championship_id_var),
            (team_uru_id, group_b_id, championship_id_var),
            (team_fra_id, group_c_id, championship_id_var),
            (team_esp_id, group_c_id, championship_id_var),
            (team_por_id, group_c_id, championship_id_var),
            (team_cro_id, group_c_id, championship_id_var),
            (team_eng_id, group_d_id, championship_id_var),
            (team_ned_id, group_d_id, championship_id_var),
            (team_ita_id, group_d_id, championship_id_var),
            (team_bel_id, group_d_id, championship_id_var)
        ON CONFLICT DO NOTHING;

        -- Insert sample matches
        INSERT INTO matches (championship_id, group_id, home_team_id, away_team_id, match_date, status, stage, venue) VALUES
            (championship_id_var, group_a_id, team_arg_id, team_usa_id, '2026-06-11 18:00:00', 'SC', 'GR', 'MetLife Stadium'),
            (championship_id_var, group_a_id, team_mex_id, team_can_id, '2026-06-11 21:00:00', 'SC', 'GR', 'Rose Bowl'),
            (championship_id_var, group_b_id, team_bra_id, team_ger_id, '2026-06-12 18:00:00', 'SC', 'GR', 'AT&T Stadium'),
            (championship_id_var, group_b_id, team_jpn_id, team_uru_id, '2026-06-12 21:00:00', 'SC', 'GR', 'Lincoln Financial Field'),
            (championship_id_var, group_c_id, team_fra_id, team_esp_id, '2026-06-13 18:00:00', 'SC', 'GR', 'Mercedes-Benz Stadium'),
            (championship_id_var, group_c_id, team_por_id, team_cro_id, '2026-06-13 21:00:00', 'SC', 'GR', 'SoFi Stadium'),
            (championship_id_var, group_d_id, team_eng_id, team_ned_id, '2026-06-14 18:00:00', 'SC', 'GR', 'Arrowhead Stadium'),
            (championship_id_var, group_d_id, team_ita_id, team_bel_id, '2026-06-14 21:00:00', 'SC', 'GR', 'Levi''s Stadium')
        ON CONFLICT DO NOTHING;
    END;
END $$;

-- Create demo users with roles
-- Password hashes generated with bcrypt cost 10
-- Demo password: demo123  -> hash: $2a$10$ttM0O473DQy0.6hqtXrMnu8dUpBBqwi7elJZHdixkoLCYlz1M0Wfm
-- Admin password: admin123 -> hash: $2a$10$Mp75WPVf60y3X2u0gN8BdOtPo.XpUxQtMsV0RGYX7hzXIU50Agxzi
INSERT INTO users (username, email, password, role) VALUES
    ('demo', 'demo@football.com', '$2a$10$ttM0O473DQy0.6hqtXrMnu8dUpBBqwi7elJZHdixkoLCYlz1M0Wfm', 'US'),
    ('admin', 'admin@football.com', '$2a$10$Mp75WPVf60y3X2u0gN8BdOtPo.XpUxQtMsV0RGYX7hzXIU50Agxzi', 'AD')
ON CONFLICT DO NOTHING;
-- Roles: US=user (regular user), AD=admin (can edit all data)
