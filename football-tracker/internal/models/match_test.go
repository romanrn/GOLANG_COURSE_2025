//go:build unit
// +build unit

package models

import (
	"football-tracker/internal/testhelpers"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMatch_GetExtraTimeScore(t *testing.T) {
	tests := []struct {
		name         string
		match        Match
		expectedHome int
		expectedAway int
	}{
		{
			name: "Match with extra time - home wins",
			match: Match{
				HomeScoreRegular: testhelpers.IntPtr(1),
				AwayScoreRegular: testhelpers.IntPtr(1),
				HomeScoreTotal:   testhelpers.IntPtr(2),
				AwayScoreTotal:   testhelpers.IntPtr(1),
				HasExtraTime:     true,
			},
			expectedHome: 1, // 2 - 1 = 1 goal in extra time
			expectedAway: 0, // 1 - 1 = 0 goals in extra time
		},
		{
			name: "Match with extra time - away wins",
			match: Match{
				HomeScoreRegular: testhelpers.IntPtr(0),
				AwayScoreRegular: testhelpers.IntPtr(0),
				HomeScoreTotal:   testhelpers.IntPtr(0),
				AwayScoreTotal:   testhelpers.IntPtr(1),
				HasExtraTime:     true,
			},
			expectedHome: 0,
			expectedAway: 1,
		},
		{
			name: "Match with extra time - both scored",
			match: Match{
				HomeScoreRegular: testhelpers.IntPtr(1),
				AwayScoreRegular: testhelpers.IntPtr(1),
				HomeScoreTotal:   testhelpers.IntPtr(3),
				AwayScoreTotal:   testhelpers.IntPtr(2),
				HasExtraTime:     true,
			},
			expectedHome: 2, // 3 - 1 = 2
			expectedAway: 1, // 2 - 1 = 1
		},
		{
			name: "Match without extra time",
			match: Match{
				HomeScoreRegular: testhelpers.IntPtr(2),
				AwayScoreRegular: testhelpers.IntPtr(1),
				HomeScoreTotal:   testhelpers.IntPtr(2),
				AwayScoreTotal:   testhelpers.IntPtr(1),
				HasExtraTime:     false,
			},
			expectedHome: 0,
			expectedAway: 0,
		},
		{
			name: "Match not finished (nil scores)",
			match: Match{
				HomeScoreRegular: nil,
				AwayScoreRegular: nil,
				HomeScoreTotal:   nil,
				AwayScoreTotal:   nil,
				HasExtraTime:     false,
			},
			expectedHome: 0,
			expectedAway: 0,
		},
		{
			name: "Extra time with no additional goals",
			match: Match{
				HomeScoreRegular: testhelpers.IntPtr(1),
				AwayScoreRegular: testhelpers.IntPtr(1),
				HomeScoreTotal:   testhelpers.IntPtr(1),
				AwayScoreTotal:   testhelpers.IntPtr(1),
				HasExtraTime:     true,
			},
			expectedHome: 0,
			expectedAway: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			homeExtra, awayExtra := tt.match.GetExtraTimeScore()

			// Assert
			assert.Equal(t, tt.expectedHome, homeExtra)
			assert.Equal(t, tt.expectedAway, awayExtra)
		})
	}
}

func TestMatch_IsGroupStage(t *testing.T) {
	tests := []struct {
		name     string
		match    Match
		expected bool
	}{
		{
			name: "Group stage match",
			match: Match{
				Stage: MatchStageGroup,
			},
			expected: true,
		},
		{
			name: "Round of 16 match",
			match: Match{
				Stage: MatchStageRoundOf16,
			},
			expected: false,
		},
		{
			name: "Quarter final match",
			match: Match{
				Stage: MatchStageQuarterFinal,
			},
			expected: false,
		},
		{
			name: "Semi final match",
			match: Match{
				Stage: MatchStageSemiFinal,
			},
			expected: false,
		},
		{
			name: "Final match",
			match: Match{
				Stage: MatchStageFinal,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			result := tt.match.IsGroupStage()

			// Assert
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatch_IsPlayoff(t *testing.T) {
	tests := []struct {
		name     string
		match    Match
		expected bool
	}{
		{
			name: "Group stage is not playoff",
			match: Match{
				Stage: MatchStageGroup,
			},
			expected: false,
		},
		{
			name: "Round of 32 is playoff",
			match: Match{
				Stage: MatchStageRoundOf32,
			},
			expected: true,
		},
		{
			name: "Round of 16 is playoff",
			match: Match{
				Stage: MatchStageRoundOf16,
			},
			expected: true,
		},
		{
			name: "Quarter final is playoff",
			match: Match{
				Stage: MatchStageQuarterFinal,
			},
			expected: true,
		},
		{
			name: "Semi final is playoff",
			match: Match{
				Stage: MatchStageSemiFinal,
			},
			expected: true,
		},
		{
			name: "Third place is playoff",
			match: Match{
				Stage: MatchStageThirdPlace,
			},
			expected: true,
		},
		{
			name: "Final is playoff",
			match: Match{
				Stage: MatchStageFinal,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			result := tt.match.IsPlayoff()

			// Assert
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatch_TableName(t *testing.T) {
	match := Match{}
	tableName := match.TableName()
	assert.Equal(t, "matches", tableName)
}

func TestMatchStatusConstants(t *testing.T) {
	// Test that constants have expected values
	assert.Equal(t, "SC", MatchStatusScheduled)
	assert.Equal(t, "LV", MatchStatusLive)
	assert.Equal(t, "FN", MatchStatusFinished)
	assert.Equal(t, "CN", MatchStatusCancelled)
	assert.Equal(t, "PP", MatchStatusPostponed)
}

func TestMatchStageConstants(t *testing.T) {
	// Test that stage constants have expected values
	assert.Equal(t, "GR", MatchStageGroup)
	assert.Equal(t, "32", MatchStageRoundOf32)
	assert.Equal(t, "16", MatchStageRoundOf16)
	assert.Equal(t, "QF", MatchStageQuarterFinal)
	assert.Equal(t, "SF", MatchStageSemiFinal)
	assert.Equal(t, "TP", MatchStageThirdPlace)
	assert.Equal(t, "FN", MatchStageFinal)
}
