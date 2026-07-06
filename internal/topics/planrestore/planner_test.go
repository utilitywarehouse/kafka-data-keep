package planrestore

import (
	"context"
	"strconv"
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
	recWith := func(value string, fileIndex int) *kgo.Record {
		rec := &kgo.Record{Value: []byte(value)}
		if fileIndex > 0 {
			rec.Headers = []kgo.RecordHeader{
				{Key: FileIndexHeader, Value: []byte(strconv.Itoa(fileIndex))},
			}
		}
		return rec
	}

	rs := func(topic, file string, fileIndex int) *resumeState {
		return &resumeState{topic: topic, file: file, fileIndex: fileIndex}
	}

	tests := []struct {
		name          string
		latestRecords map[int32]*kgo.Record
		topicsOrder   []string
		want          *resumeState
	}{
		{
			name: "single topic",
			latestRecords: map[int32]*kgo.Record{
				0: recWith("kafka-backup/account-identity.account.change.events/0/account-identity.account.change.events-0-0000000000000000001.avro", 0),
				1: recWith("kafka-backup/account-identity.account.change.events/11/account-identity.account.change.events-11-0000000000009153963.avro", 0),
				2: recWith("kafka-backup/account-identity.account.change.events/12/account-identity.account.change.events-12-0000000000008519936.avro", 0),
				3: recWith("kafka-backup/account-identity.account.change.events/12/account-identity.account.change.events-12-0000000000009082875.avro", 0),
			},
			topicsOrder: []string{
				"topic-not-in-list",
				"account-identity.account.change.events",
			},
			want: rs("account-identity.account.change.events",
				"kafka-backup/account-identity.account.change.events/12/account-identity.account.change.events-12-0000000000009082875.avro", 0),
		},
		{
			name: "multiple topics",
			latestRecords: map[int32]*kgo.Record{
				0: recWith("kafka-backup/account-identity.account.change.events/0/account-identity.account.change.events-0-0000000000000000001.avro", 0),
				1: recWith("kafka-backup/account-identity.account.change.events/11/account-identity.account.change.events-11-0000000000009153963.avro", 0),
				2: recWith("kafka-backup/account-identity.account.change.events/12/account-identity.account.change.events-12-0000000000008519936.avro", 0),
				3: recWith("kafka-backup/cbc.PaymentologyNotificationEvents/0/cbc.PaymentologyNotificationEvents-0-0000000000049607695.avro", 0),
				4: recWith("kafka-backup/cbc.PaymentologyNotificationEvents/11/cbc.PaymentologyNotificationEvents-11-0000000000006148155.avro", 0),
				5: recWith("kafka-backup/cbc.PaymentologyNotificationEvents/3/cbc.PaymentologyNotificationEvents-3-0000000000023826482.avro", 0),
				6: recWith("kafka-backup/cbc.PaymentologyNotificationEvents/2/cbc.PaymentologyNotificationEvents-2-0000000000049854163.avro", 0),
			},
			topicsOrder: []string{
				"cbc.PaymentologyNotificationEvents", "account-identity.account.change.events",
			},
			want: rs("account-identity.account.change.events",
				"kafka-backup/account-identity.account.change.events/12/account-identity.account.change.events-12-0000000000008519936.avro", 0),
		},
		{
			name: "no matching topic in order",
			latestRecords: map[int32]*kgo.Record{
				2: recWith("kafka-backup/account-identity.account.change.events/12/account-identity.account.change.events-12-0000000000008519936.avro", 0),
				3: recWith("kafka-backup/cbc.PaymentologyNotificationEvents/0/cbc.PaymentologyNotificationEvents-0-0000000000049607695.avro", 0),
			},
			topicsOrder: []string{"another-topic"},
			want:        nil,
		},
		{
			name:          "no last entries",
			latestRecords: nil,
			topicsOrder: []string{
				"cbc.PaymentologyNotificationEvents", "account-identity.account.change.events",
			},
			want: nil,
		},
		{
			name: "file-index header is extracted from the resume record",
			latestRecords: map[int32]*kgo.Record{
				0: recWith("kafka-backup/topic-a/0/topic-a-0-0000000000000000005.avro", 5),
			},
			topicsOrder: []string{"topic-a"},
			want:        rs("topic-a", "kafka-backup/topic-a/0/topic-a-0-0000000000000000005.avro", 5),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeResume(tt.latestRecords, tt.topicsOrder)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestReorderLargeTopicsLast(t *testing.T) {
	ctx := context.Background()
	mb := int64(1024 * 1024)

	smallInfo := &topicInfo{sizeBytes: 10 * mb, partitionCounts: map[string]int{}}
	largeInfo := &topicInfo{sizeBytes: 200 * mb, partitionCounts: map[string]int{}}
	threshold := 100 * mb

	tests := []struct {
		name     string
		topics   []string
		info     map[string]*topicInfo
		expected []string
	}{
		{
			name:     "all small",
			topics:   []string{"a", "b", "c"},
			info:     map[string]*topicInfo{"a": smallInfo, "b": smallInfo, "c": smallInfo},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "all large",
			topics:   []string{"a", "b", "c"},
			info:     map[string]*topicInfo{"a": largeInfo, "b": largeInfo, "c": largeInfo},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "mixed: large moved to end, order preserved within groups",
			topics:   []string{"small-a", "large-b", "small-c", "large-d"},
			info:     map[string]*topicInfo{"small-a": smallInfo, "large-b": largeInfo, "small-c": smallInfo, "large-d": largeInfo},
			expected: []string{"small-a", "small-c", "large-b", "large-d"},
		},
		{
			name:     "boundary: exactly at threshold is not large",
			topics:   []string{"a"},
			info:     map[string]*topicInfo{"a": {sizeBytes: threshold, partitionCounts: map[string]int{}}},
			expected: []string{"a"},
		},
		{
			name:     "topic with missing info treated as small",
			topics:   []string{"known", "unknown"},
			info:     map[string]*topicInfo{"known": largeInfo},
			expected: []string{"unknown", "known"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := reorderLargeTopicsLast(ctx, tt.topics, tt.info, threshold)
			assert.Equal(t, tt.expected, got)
		})
	}
}
