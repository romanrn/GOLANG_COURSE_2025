package repositories

import (
	"context"
	"fmt"
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/clients/database"
	"football-tracker/internal/models/dto"
	"log/slog"
)

type matchRepository struct {
	db *database.PostgresClient
}

func NewMatchRepository(dbClient *database.PostgresClient) MatchRepository {
	return &matchRepository{
		db: dbClient,
	}
}

func (r *matchRepository) GetByChampionshipId(ctx context.Context, championshipId int) ([]dto.MatchDTO, error) {
	logger.GetLogger().Info(
		ctx,
		"Querying matches for championship from database",
		slog.String("component", "repository"),
		slog.String("method", "GetByChampionshipId"),
		slog.Int("championship_id", championshipId),
	)

	query := `
		SELECT m.id, m.championship_id, m.group_id, m.match_date,
		       m.home_score_regular, m.away_score_regular, m.home_score_total, m.away_score_total,
		       m.has_extra_time, m.has_penalty, m.status, m.stage, m.venue, m.city_id,
		       m.created_at, m.updated_at,
		       ht.id, ht.name, ht.country_code, ht.country_name, ht.flag_url,
		       at.id, at.name, at.country_code, at.country_name, at.flag_url
		FROM matches m
		INNER JOIN teams ht ON m.home_team_id = ht.id
		INNER JOIN teams at ON m.away_team_id = at.id
		WHERE m.championship_id = $1
		ORDER BY m.match_date ASC, m.id ASC
	`

	rows, err := r.db.DB.QueryContext(ctx, query, championshipId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to execute query",
			slog.String("component", "repository"),
			slog.String("method", "GetByChampionshipId"),
			slog.Int("championship_id", championshipId),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to query matches: %w", err)
	}
	defer rows.Close()

	var matches []dto.MatchDTO

	for rows.Next() {
		var matchDTO dto.MatchDTO
		var result dto.MatchResult

		err := rows.Scan(
			&matchDTO.ID,
			&matchDTO.ChampionshipID,
			&matchDTO.GroupID,
			&matchDTO.MatchDate,
			&result.HomeScoreRegular,
			&result.AwayScoreRegular,
			&result.HomeScoreTotal,
			&result.AwayScoreTotal,
			&matchDTO.HasExtraTime,
			&matchDTO.HasPenalty,
			&matchDTO.Status,
			&matchDTO.Stage,
			&matchDTO.Venue,
			&matchDTO.CityID,
			&matchDTO.CreatedAt,
			&matchDTO.UpdatedAt,
			&matchDTO.HomeTeam.ID,
			&matchDTO.HomeTeam.Name,
			&matchDTO.HomeTeam.CountryCode,
			&matchDTO.HomeTeam.CountryName,
			&matchDTO.HomeTeam.FlagURL,
			&matchDTO.AwayTeam.ID,
			&matchDTO.AwayTeam.Name,
			&matchDTO.AwayTeam.CountryCode,
			&matchDTO.AwayTeam.CountryName,
			&matchDTO.AwayTeam.FlagURL,
		)
		if err != nil {
			logger.GetLogger().Error(
				ctx,
				"Failed to scan match row",
				slog.String("component", "repository"),
				slog.String("method", "GetByChampionshipId"),
				slog.String("error", err.Error()),
			)
			return nil, fmt.Errorf("failed to scan match: %w", err)
		}

		// Always set result with formatted score
		result.Score = dto.FormatScore(
			result.HomeScoreRegular,
			result.AwayScoreRegular,
			result.HomeScoreTotal,
			result.AwayScoreTotal,
			matchDTO.HasExtraTime,
		)
		matchDTO.Result = &result

		matches = append(matches, matchDTO)
	}

	if err = rows.Err(); err != nil {
		logger.GetLogger().Error(
			ctx,
			"Error iterating match rows",
			slog.String("component", "repository"),
			slog.String("method", "GetByChampionshipId"),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("error iterating matches: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully queried matches from database",
		slog.String("component", "repository"),
		slog.String("method", "GetByChampionshipId"),
		slog.Int("championship_id", championshipId),
		slog.Int("count", len(matches)),
	)

	return matches, nil
}

func (r *matchRepository) GetById(ctx context.Context, matchId int) (dto.MatchDTO, error) {
	logger.GetLogger().Info(
		ctx,
		"Querying match by ID from database",
		slog.String("component", "repository"),
		slog.String("method", "GetById"),
		slog.Int("match_id", matchId),
	)

	query := `
		SELECT m.id, m.championship_id, m.group_id, m.match_date,
		       m.home_score_regular, m.away_score_regular, m.home_score_total, m.away_score_total,
		       m.has_extra_time, m.has_penalty, m.status, m.stage, m.venue, m.city_id,
		       m.created_at, m.updated_at,
		       ht.id, ht.name, ht.country_code, ht.country_name, ht.flag_url,
		       at.id, at.name, at.country_code, at.country_name, at.flag_url
		FROM matches m
		INNER JOIN teams ht ON m.home_team_id = ht.id
		INNER JOIN teams at ON m.away_team_id = at.id
		WHERE m.id = $1
	`

	var matchDTO dto.MatchDTO
	var result dto.MatchResult

	err := r.db.DB.QueryRowContext(ctx, query, matchId).Scan(
		&matchDTO.ID,
		&matchDTO.ChampionshipID,
		&matchDTO.GroupID,
		&matchDTO.MatchDate,
		&result.HomeScoreRegular,
		&result.AwayScoreRegular,
		&result.HomeScoreTotal,
		&result.AwayScoreTotal,
		&matchDTO.HasExtraTime,
		&matchDTO.HasPenalty,
		&matchDTO.Status,
		&matchDTO.Stage,
		&matchDTO.Venue,
		&matchDTO.CityID,
		&matchDTO.CreatedAt,
		&matchDTO.UpdatedAt,
		&matchDTO.HomeTeam.ID,
		&matchDTO.HomeTeam.Name,
		&matchDTO.HomeTeam.CountryCode,
		&matchDTO.HomeTeam.CountryName,
		&matchDTO.HomeTeam.FlagURL,
		&matchDTO.AwayTeam.ID,
		&matchDTO.AwayTeam.Name,
		&matchDTO.AwayTeam.CountryCode,
		&matchDTO.AwayTeam.CountryName,
		&matchDTO.AwayTeam.FlagURL,
	)

	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get match by ID",
			slog.String("component", "repository"),
			slog.String("method", "GetById"),
			slog.Int("match_id", matchId),
			slog.String("error", err.Error()),
		)
		return dto.MatchDTO{}, fmt.Errorf("failed to get match by ID %d: %w", matchId, err)
	}

	// Always set result with formatted score
	result.Score = dto.FormatScore(
		result.HomeScoreRegular,
		result.AwayScoreRegular,
		result.HomeScoreTotal,
		result.AwayScoreTotal,
		matchDTO.HasExtraTime,
	)
	matchDTO.Result = &result

	logger.GetLogger().Info(
		ctx,
		"Successfully retrieved match by ID",
		slog.String("component", "repository"),
		slog.String("method", "GetById"),
		slog.Int("match_id", matchId),
		slog.String("status", matchDTO.Status),
		slog.String("stage", matchDTO.Stage),
	)

	return matchDTO, nil
}
