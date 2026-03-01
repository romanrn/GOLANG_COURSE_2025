//go:build unit
// +build unit

package dto

import (
	"football-tracker/internal/testhelpers"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatScore(t *testing.T) {
	tests := []struct {
		name         string
		homeRegular  *int
		awayRegular  *int
		homeTotal    *int
		awayTotal    *int
		hasExtraTime bool
		expected     string
	}{
		{
			name:         "Regular time score without extra time",
			homeRegular:  testhelpers.IntPtr(2),
			awayRegular:  testhelpers.IntPtr(1),
			homeTotal:    nil,
			awayTotal:    nil,
			hasExtraTime: false,
			expected:     "2:1",
		},
		{
			name:         "Draw without extra time",
			homeRegular:  testhelpers.IntPtr(0),
			awayRegular:  testhelpers.IntPtr(0),
			homeTotal:    nil,
			awayTotal:    nil,
			hasExtraTime: false,
			expected:     "0:0",
		},
		{
			name:         "Score with extra time",
			homeRegular:  testhelpers.IntPtr(1),
			awayRegular:  testhelpers.IntPtr(1),
			homeTotal:    testhelpers.IntPtr(2),
			awayTotal:    testhelpers.IntPtr(1),
			hasExtraTime: true,
			expected:     "1:1 (2:1)",
		},
		{
			name:         "Nil scores default to 0:0",
			homeRegular:  nil,
			awayRegular:  nil,
			homeTotal:    nil,
			awayTotal:    nil,
			hasExtraTime: false,
			expected:     "0:0",
		},
		{
			name:         "Extra time flag but no total scores",
			homeRegular:  testhelpers.IntPtr(1),
			awayRegular:  testhelpers.IntPtr(1),
			homeTotal:    nil,
			awayTotal:    nil,
			hasExtraTime: true,
			expected:     "1:1",
		},
		{
			name:         "Total scores provided but no extra time flag",
			homeRegular:  testhelpers.IntPtr(1),
			awayRegular:  testhelpers.IntPtr(1),
			homeTotal:    testhelpers.IntPtr(2),
			awayTotal:    testhelpers.IntPtr(1),
			hasExtraTime: false,
			expected:     "1:1",
		},
		{
			name:         "High score match",
			homeRegular:  testhelpers.IntPtr(5),
			awayRegular:  testhelpers.IntPtr(4),
			homeTotal:    nil,
			awayTotal:    nil,
			hasExtraTime: false,
			expected:     "5:4",
		},
		{
			name:         "Extra time with penalty shootout score",
			homeRegular:  testhelpers.IntPtr(0),
			awayRegular:  testhelpers.IntPtr(0),
			homeTotal:    testhelpers.IntPtr(0),
			awayTotal:    testhelpers.IntPtr(0),
			hasExtraTime: true,
			expected:     "0:0 (0:0)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			result := FormatScore(tt.homeRegular, tt.awayRegular, tt.homeTotal, tt.awayTotal, tt.hasExtraTime)

			// Assert
			assert.Equal(t, tt.expected, result)
		})
	}
}
