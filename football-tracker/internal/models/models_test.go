//go:build unit
// +build unit

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChampionship_TableName(t *testing.T) {
	championship := Championship{}
	tableName := championship.TableName()
	assert.Equal(t, "championships", tableName)
}

func TestTeam_TableName(t *testing.T) {
	team := Team{}
	tableName := team.TableName()
	assert.Equal(t, "teams", tableName)
}
