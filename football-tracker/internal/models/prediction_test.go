//go:build unit
// +build unit

package models

import (
	"football-tracker/internal/testhelpers"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrediction_CalculatePoints(t *testing.T) {
	tests := []struct {
		name           string
		prediction     Prediction
		match          Match
		expectedPoints int
	}{
		{
			name: "Exact score prediction - 3 points",
			prediction: Prediction{
				HomeScoreRegular: 2,
				AwayScoreRegular: 1,
			},
			match: Match{
				HomeScoreRegular: testhelpers.IntPtr(2),
				AwayScoreRegular: testhelpers.IntPtr(1),
			},
			expectedPoints: PointsExactScore,
		},
		{
			name: "Correct outcome but wrong score - 1 point (home win)",
			prediction: Prediction{
				HomeScoreRegular: 2,
				AwayScoreRegular: 0,
			},
			match: Match{
				HomeScoreRegular: testhelpers.IntPtr(3),
				AwayScoreRegular: testhelpers.IntPtr(1),
			},
			expectedPoints: PointsCorrectOutcome,
		},
		{
			name: "Correct outcome but wrong score - 1 point (away win)",
			prediction: Prediction{
				HomeScoreRegular: 0,
				AwayScoreRegular: 2,
			},
			match: Match{
				HomeScoreRegular: testhelpers.IntPtr(1),
				AwayScoreRegular: testhelpers.IntPtr(3),
			},
			expectedPoints: PointsCorrectOutcome,
		},
		{
			name: "Correct outcome but wrong score - 1 point (draw)",
			prediction: Prediction{
				HomeScoreRegular: 1,
				AwayScoreRegular: 1,
			},
			match: Match{
				HomeScoreRegular: testhelpers.IntPtr(2),
				AwayScoreRegular: testhelpers.IntPtr(2),
			},
			expectedPoints: PointsCorrectOutcome,
		},
		{
			name: "Wrong prediction - 0 points",
			prediction: Prediction{
				HomeScoreRegular: 2,
				AwayScoreRegular: 0,
			},
			match: Match{
				HomeScoreRegular: testhelpers.IntPtr(0),
				AwayScoreRegular: testhelpers.IntPtr(2),
			},
			expectedPoints: PointsWrongPrediction,
		},
		{
			name: "Match not finished yet - 0 points",
			prediction: Prediction{
				HomeScoreRegular: 2,
				AwayScoreRegular: 1,
			},
			match: Match{
				HomeScoreRegular: nil,
				AwayScoreRegular: nil,
			},
			expectedPoints: 0,
		},
		{
			name: "Exact score + bonus for exact total score",
			prediction: Prediction{
				HomeScoreRegular: 1,
				AwayScoreRegular: 1,
				HomeScoreTotal:   testhelpers.IntPtr(2),
				AwayScoreTotal:   testhelpers.IntPtr(1),
			},
			match: Match{
				HomeScoreRegular: testhelpers.IntPtr(1),
				AwayScoreRegular: testhelpers.IntPtr(1),
				HomeScoreTotal:   testhelpers.IntPtr(2),
				AwayScoreTotal:   testhelpers.IntPtr(1),
				HasExtraTime:     true,
			},
			expectedPoints: PointsExactScore + BonusExactTotal,
		},
		{
			name: "Correct outcome but wrong total score - no bonus",
			prediction: Prediction{
				HomeScoreRegular: 1,
				AwayScoreRegular: 1,
				HomeScoreTotal:   testhelpers.IntPtr(2),
				AwayScoreTotal:   testhelpers.IntPtr(1),
			},
			match: Match{
				HomeScoreRegular: testhelpers.IntPtr(1),
				AwayScoreRegular: testhelpers.IntPtr(1),
				HomeScoreTotal:   testhelpers.IntPtr(3),
				AwayScoreTotal:   testhelpers.IntPtr(1),
				HasExtraTime:     true,
			},
			expectedPoints: PointsExactScore, // Only exact regular time score
		},
		{
			name: "Total score prediction without extra time - no bonus",
			prediction: Prediction{
				HomeScoreRegular: 2,
				AwayScoreRegular: 1,
				HomeScoreTotal:   testhelpers.IntPtr(2),
				AwayScoreTotal:   testhelpers.IntPtr(1),
			},
			match: Match{
				HomeScoreRegular: testhelpers.IntPtr(2),
				AwayScoreRegular: testhelpers.IntPtr(1),
				HomeScoreTotal:   testhelpers.IntPtr(2),
				AwayScoreTotal:   testhelpers.IntPtr(1),
				HasExtraTime:     false,
			},
			expectedPoints: PointsExactScore, // No bonus without extra time
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			points := tt.prediction.CalculatePoints(&tt.match)

			// Assert
			assert.Equal(t, tt.expectedPoints, points)
		})
	}
}

func TestPrediction_getOutcome(t *testing.T) {
	prediction := Prediction{}

	tests := []struct {
		name            string
		homeScore       int
		awayScore       int
		expectedOutcome int
	}{
		{
			name:            "Home win",
			homeScore:       2,
			awayScore:       1,
			expectedOutcome: 1,
		},
		{
			name:            "Away win",
			homeScore:       1,
			awayScore:       2,
			expectedOutcome: -1,
		},
		{
			name:            "Draw",
			homeScore:       1,
			awayScore:       1,
			expectedOutcome: 0,
		},
		{
			name:            "Draw 0-0",
			homeScore:       0,
			awayScore:       0,
			expectedOutcome: 0,
		},
		{
			name:            "Large home win",
			homeScore:       5,
			awayScore:       0,
			expectedOutcome: 1,
		},
		{
			name:            "Large away win",
			homeScore:       0,
			awayScore:       5,
			expectedOutcome: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			outcome := prediction.getOutcome(tt.homeScore, tt.awayScore)

			// Assert
			assert.Equal(t, tt.expectedOutcome, outcome)
		})
	}
}
