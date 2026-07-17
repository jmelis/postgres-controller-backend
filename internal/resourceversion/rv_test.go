package resourceversion

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		rv   RV
		want string
	}{
		{
			name: "watermark only",
			rv:   RV{Watermark: 12345678},
			want: "12345678",
		},
		{
			name: "zero watermark",
			rv:   RV{Watermark: 0},
			want: "0",
		},
		{
			name: "with object version",
			rv:   RV{ObjectVersion: 5, Watermark: 42},
			want: "o5;42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.rv.String()
			assert.Equal(t, tt.want, s)

			parsed, err := Parse(s)
			require.NoError(t, err)
			assert.Equal(t, tt.rv.Watermark, parsed.Watermark)
			assert.Equal(t, tt.rv.ObjectVersion, parsed.ObjectVersion)
		})
	}
}

func TestParseEmpty(t *testing.T) {
	rv, err := Parse("")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), rv.Watermark)
	assert.Equal(t, int64(0), rv.ObjectVersion)
}

func TestParseErrors(t *testing.T) {
	bad := []string{
		"abc",
		"o;42",
		"oabc;42",
		"o5;",
		"o5;abc",
	}
	for _, s := range bad {
		_, err := Parse(s)
		assert.Error(t, err, "expected error for %q", s)
	}
}

func TestOldBucketFormatRejected(t *testing.T) {
	_, err := Parse("b2:1044")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "old bucket-based format")
}

func TestStringDeterministic(t *testing.T) {
	rv := RV{ObjectVersion: 3, Watermark: 99999}
	for i := 0; i < 100; i++ {
		assert.Equal(t, "o3;99999", rv.String())
	}
}
