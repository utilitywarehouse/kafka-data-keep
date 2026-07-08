package restore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestHeaderValue(t *testing.T) {
	headers := []kgo.RecordHeader{
		{Key: "plan-restore.file-index", Value: []byte("3")},
		{Key: "plan-restore.total-files", Value: []byte("10")},
	}

	t.Run("present", func(t *testing.T) {
		val, ok := headerValue(headers, "plan-restore.file-index")
		assert.True(t, ok)
		assert.Equal(t, "3", val)
	})

	t.Run("second header present", func(t *testing.T) {
		val, ok := headerValue(headers, "plan-restore.total-files")
		assert.True(t, ok)
		assert.Equal(t, "10", val)
	})

	t.Run("missing", func(t *testing.T) {
		_, ok := headerValue(headers, "does-not-exist")
		assert.False(t, ok)
	})

	t.Run("empty headers", func(t *testing.T) {
		_, ok := headerValue(nil, "plan-restore.file-index")
		assert.False(t, ok)
	})
}
