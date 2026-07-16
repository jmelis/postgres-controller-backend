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
			name: "single bucket",
			rv:   RV{Buckets: map[int]int64{2: 1044}},
			want: "b2:1044",
		},
		{
			name: "multiple buckets sorted",
			rv:   RV{Buckets: map[int]int64{9: 4123, 2: 1044, 5: 902}},
			want: "b2:1044,b5:902,b9:4123",
		},
		{
			name: "empty buckets",
			rv:   RV{Buckets: map[int]int64{}},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.rv.String()
			assert.Equal(t, tt.want, s)

			parsed, err := Parse(s)
			require.NoError(t, err)
			assert.Equal(t, tt.rv.Buckets, parsed.Buckets)
		})
	}
}

func TestParseCanonical(t *testing.T) {
	rv, err := Parse("b2:1044,b5:902,b9:4123")
	require.NoError(t, err)
	assert.Equal(t, int64(1044), rv.Buckets[2])
	assert.Equal(t, int64(902), rv.Buckets[5])
	assert.Equal(t, int64(4123), rv.Buckets[9])
}

func TestParseErrors(t *testing.T) {
	bad := []string{
		"x2:1044",
		"b2",
		"b2:abc",
	}
	for _, s := range bad {
		_, err := Parse(s)
		assert.Error(t, err, "expected error for %q", s)
	}
}

func TestObjectVersionRoundTrip(t *testing.T) {
	rv := RV{ObjectVersion: 5, Buckets: map[int]int64{0: 42, 1: 17}}
	s := rv.String()
	assert.Equal(t, "o5;b0:42,b1:17", s)

	parsed, err := Parse(s)
	require.NoError(t, err)
	assert.Equal(t, int64(5), parsed.ObjectVersion)
	assert.Equal(t, rv.Buckets, parsed.Buckets)
}

func TestParseWithoutObjectVersion(t *testing.T) {
	rv, err := Parse("b0:42,b1:17")
	require.NoError(t, err)
	assert.Equal(t, int64(0), rv.ObjectVersion)
}

func TestObjectVersionParseErrors(t *testing.T) {
	bad := []string{
		"o;b0:42",
		"oabc;b0:42",
		"o5;",
		"o5;x0:42",
	}
	for _, s := range bad {
		_, err := Parse(s)
		assert.Error(t, err, "expected error for %q", s)
	}
}

func TestStringDeterministic(t *testing.T) {
	rv := RV{Buckets: map[int]int64{3: 10, 1: 20, 2: 30}}
	for i := 0; i < 100; i++ {
		assert.Equal(t, "b1:20,b2:30,b3:10", rv.String())
	}
}
