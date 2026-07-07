package resourceversion

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// RV is a composite resourceVersion: per-bucket high-water map.
// Serialized form: "b2:1044,b5:902,b9:4123"
type RV struct {
	Buckets map[int]int64
}

func (rv RV) String() string {
	keys := make([]int, 0, len(rv.Buckets))
	for k := range rv.Buckets {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("b%d:%d", k, rv.Buckets[k])
	}

	return strings.Join(parts, ",")
}

func Parse(s string) (RV, error) {
	rv := RV{Buckets: make(map[int]int64)}

	if s == "" {
		return rv, nil
	}

	for _, part := range strings.Split(s, ",") {
		colonIdx := strings.IndexByte(part, ':')
		if colonIdx < 0 || !strings.HasPrefix(part, "b") {
			return rv, fmt.Errorf("invalid RV %q: bad bucket entry %q", s, part)
		}
		bucketID, err := strconv.Atoi(part[1:colonIdx])
		if err != nil {
			return rv, fmt.Errorf("invalid RV %q: bad bucket id in %q: %w", s, part, err)
		}
		seq, err := strconv.ParseInt(part[colonIdx+1:], 10, 64)
		if err != nil {
			return rv, fmt.Errorf("invalid RV %q: bad seq in %q: %w", s, part, err)
		}
		rv.Buckets[bucketID] = seq
	}

	return rv, nil
}
