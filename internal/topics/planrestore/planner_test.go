package planrestore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/utilitywarehouse/kafka-data-keep/internal/kafka"
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
		{
			name:        "Topic matches multiple include regexes",
			topicsRegex: `accounting, accounting-support\.`,
			inputTopics: []string{"accounting-support.test", "accounting.topic2", "pubsub.topic1", "pubsub.topic2", "pubsub.not-needed"},
			expected:    []string{"accounting-support.test", "accounting.topic2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &planner{
				cfg: AppConfig{
					KafkaConfig: kafka.Config{
						Brokers: "localhost:9092",
					},
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

func TestComputeResume(t *testing.T) {
	tests := []struct {
		name          string
		latestRecords map[int32]*kgo.Record
		topicsOrder   []string
		wantTopic     string
		wantFile      string
	}{
		{
			name: "single topic",
			latestRecords: map[int32]*kgo.Record{
				0: {Value: []byte("kafka-backup/account-identity.account.change.events/0/account-identity.account.change.events-0-0000000000000000001.avro")},
				1: {Value: []byte("kafka-backup/account-identity.account.change.events/11/account-identity.account.change.events-11-0000000000009153963.avro")},
				2: {Value: []byte("kafka-backup/account-identity.account.change.events/12/account-identity.account.change.events-12-0000000000008519936.avro")},
				3: {Value: []byte("kafka-backup/account-identity.account.change.events/12/account-identity.account.change.events-12-0000000000009082875.avro")},
			},
			topicsOrder: []string{
				"topic-not-in-list",
				"account-identity.account.change.events",
			},
			wantTopic: "account-identity.account.change.events",
			wantFile:  "kafka-backup/account-identity.account.change.events/12/account-identity.account.change.events-12-0000000000009082875.avro",
		},
		{
			name: "multiple topics",
			latestRecords: map[int32]*kgo.Record{
				0: {Value: []byte("kafka-backup/account-identity.account.change.events/0/account-identity.account.change.events-0-0000000000000000001.avro")},
				1: {Value: []byte("kafka-backup/account-identity.account.change.events/11/account-identity.account.change.events-11-0000000000009153963.avro")},
				2: {Value: []byte("kafka-backup/account-identity.account.change.events/12/account-identity.account.change.events-12-0000000000008519936.avro")},
				3: {Value: []byte("kafka-backup/cbc.PaymentologyNotificationEvents/0/cbc.PaymentologyNotificationEvents-0-0000000000049607695.avro")},
				4: {Value: []byte("kafka-backup/cbc.PaymentologyNotificationEvents/11/cbc.PaymentologyNotificationEvents-11-0000000000006148155.avro")},
				5: {Value: []byte("kafka-backup/cbc.PaymentologyNotificationEvents/3/cbc.PaymentologyNotificationEvents-3-0000000000023826482.avro")},
				6: {Value: []byte("kafka-backup/cbc.PaymentologyNotificationEvents/2/cbc.PaymentologyNotificationEvents-2-0000000000049854163.avro")},
			},
			topicsOrder: []string{
				"cbc.PaymentologyNotificationEvents", "account-identity.account.change.events",
			},
			wantTopic: "account-identity.account.change.events",
			wantFile:  "kafka-backup/account-identity.account.change.events/12/account-identity.account.change.events-12-0000000000008519936.avro",
		},
		{
			name: "restore other topics",
			latestRecords: map[int32]*kgo.Record{
				2: {Value: []byte("kafka-backup/account-identity.account.change.events/12/account-identity.account.change.events-12-0000000000008519936.avro")},
				3: {Value: []byte("kafka-backup/cbc.PaymentologyNotificationEvents/0/cbc.PaymentologyNotificationEvents-0-0000000000049607695.avro")},
			},
			topicsOrder: []string{
				"another-topic",
			},
			wantTopic: "",
			wantFile:  "",
		},
		{
			name:          "no last entries",
			latestRecords: nil,
			topicsOrder: []string{
				"cbc.PaymentologyNotificationEvents", "account-identity.account.change.events",
			},
			wantTopic: "",
			wantFile:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic, file, err := computeResume(tt.latestRecords, tt.topicsOrder)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTopic, topic)
			assert.Equal(t, tt.wantFile, file)
		})
	}
}
