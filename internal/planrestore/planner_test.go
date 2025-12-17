package planrestore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanner_filterTopics(t *testing.T) {
	tests := []struct {
		name               string
		topicsRegex        string
		excludeTopicsRegex string
		inputTopics        []string
		expected           []string
	}{
		{
			name:        "Default include all",
			topicsRegex: ".*",
			inputTopics: []string{"topic1", "topic2"},
			expected:    []string{"topic1", "topic2"},
		},
		{
			name:        "Include specific matches",
			topicsRegex: "topic1, topic2",
			inputTopics: []string{"topic1", "topic2", "topic3"},
			expected:    []string{"topic1", "topic2"},
		},
		{
			name:        "Include partial match",
			topicsRegex: "topic",
			inputTopics: []string{"topic1", "other", "topic2"},
			expected:    []string{"topic1", "topic2"},
		},
		{
			name:               "Exclude specific match",
			topicsRegex:        ".*",
			excludeTopicsRegex: "topic2",
			inputTopics:        []string{"topic1", "topic2", "topic3"},
			expected:           []string{"topic1", "topic3"},
		},
		{
			name:               "Complex include and exclude",
			topicsRegex:        "^prod-.*",
			excludeTopicsRegex: ".*-secret$",
			inputTopics:        []string{"prod-app", "prod-db-secret", "dev-app"},
			expected:           []string{"prod-app"},
		},
		{
			name:               "Multiple include regexes in regex order",
			topicsRegex:        `pubsub\., accounting\.`,
			excludeTopicsRegex: `pubsub\.not-needed`,
			inputTopics:        []string{"accounting.topic1", "accounting.topic2", "pubsub.topic1", "pubsub.topic2", "pubsub.not-needed"},
			expected:           []string{"pubsub.topic1", "pubsub.topic2", "accounting.topic1", "accounting.topic2"},
		},
		{
			name:               "Multiple exclude regexes",
			topicsRegex:        ".*",
			excludeTopicsRegex: "^a.*,^c.*",
			inputTopics:        []string{"apple", "banana", "cherry"},
			expected:           []string{"banana"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Planner{
				cfg: AppConfig{
					RestoreTopicsRegex: tt.topicsRegex,
					ExcludeTopicsRegex: tt.excludeTopicsRegex,
				},
			}

			got, err := p.filterTopics(tt.inputTopics)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}
