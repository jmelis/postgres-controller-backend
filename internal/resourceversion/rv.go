package resourceversion

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// RV is a composite resourceVersion: per-bucket high-water map.
// Serialized form: "b2:1044,b5:902,b9:4123"
//
// When stamped on a single object delivered by a watch event, the object's own
// version is carried as an "o<version>;" prefix: "o5;b2:1044,b5:902". This
// keeps the RV parseable for watch resumption while letting write paths
// recover the object version for optimistic concurrency.
type RV struct {
	// ObjectVersion is the per-object version (kubernetes_resources.object_version)
	// of the object this RV was stamped on. Zero for collection-level RVs.
	ObjectVersion int64
	Buckets       map[int]int64
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

	s := strings.Join(parts, ",")
	if rv.ObjectVersion > 0 {
		return fmt.Sprintf("o%d;%s", rv.ObjectVersion, s)
	}
	return s
}

func Parse(s string) (RV, error) {
	rv := RV{Buckets: make(map[int]int64)}

	if s == "" {
		return rv, nil
	}

	if strings.HasPrefix(s, "o") {
		semiIdx := strings.IndexByte(s, ';')
		if semiIdx < 0 {
			return rv, fmt.Errorf("invalid RV %q: object version prefix without buckets", s)
		}
		ov, err := strconv.ParseInt(s[1:semiIdx], 10, 64)
		if err != nil {
			return rv, fmt.Errorf("invalid RV %q: bad object version: %w", s, err)
		}
		rv.ObjectVersion = ov
		s = s[semiIdx+1:]
		if s == "" {
			return rv, fmt.Errorf("invalid RV: object version prefix with empty buckets")
		}
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
