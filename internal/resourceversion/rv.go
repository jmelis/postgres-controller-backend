package resourceversion

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// RV is a composite resourceVersion: epoch prefix + per-bucket high-water map.
// Serialized form: "e7|b2:1044,b5:902,b9:4123"
type RV struct {
	Epoch   int64
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

	return fmt.Sprintf("e%d|%s", rv.Epoch, strings.Join(parts, ","))
}

func Parse(s string) (RV, error) {
	rv := RV{Buckets: make(map[int]int64)}

	pipeIdx := strings.IndexByte(s, '|')
	if pipeIdx < 0 {
		return rv, fmt.Errorf("invalid RV %q: missing '|'", s)
	}

	epochStr := s[:pipeIdx]
	if !strings.HasPrefix(epochStr, "e") {
		return rv, fmt.Errorf("invalid RV %q: epoch must start with 'e'", s)
	}
	epoch, err := strconv.ParseInt(epochStr[1:], 10, 64)
	if err != nil {
		return rv, fmt.Errorf("invalid RV %q: bad epoch: %w", s, err)
	}
	rv.Epoch = epoch

	bucketStr := s[pipeIdx+1:]
	if bucketStr == "" {
		return rv, nil
	}

	for _, part := range strings.Split(bucketStr, ",") {
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
