package resourceversion

import (
	"fmt"
	"strconv"
	"strings"
)

// RV is a resourceVersion backed by a single xid8 watermark.
// Serialized form: "12345678" (collection-level) or "o5;12345678" (object-level,
// carrying the per-object version for optimistic concurrency).
type RV struct {
	ObjectVersion int64
	Watermark     uint64
}

func (rv RV) String() string {
	if rv.ObjectVersion > 0 {
		return fmt.Sprintf("o%d;%d", rv.ObjectVersion, rv.Watermark)
	}
	return strconv.FormatUint(rv.Watermark, 10)
}

func Parse(s string) (RV, error) {
	var rv RV

	if s == "" {
		return rv, nil
	}

	if strings.HasPrefix(s, "o") {
		semiIdx := strings.IndexByte(s, ';')
		if semiIdx < 0 {
			return rv, fmt.Errorf("invalid RV %q: object version prefix without watermark", s)
		}
		ov, err := strconv.ParseInt(s[1:semiIdx], 10, 64)
		if err != nil {
			return rv, fmt.Errorf("invalid RV %q: bad object version: %w", s, err)
		}
		rv.ObjectVersion = ov
		s = s[semiIdx+1:]
		if s == "" {
			return rv, fmt.Errorf("invalid RV: object version prefix with empty watermark")
		}
	}

	if strings.HasPrefix(s, "b") {
		return rv, fmt.Errorf("invalid RV %q: old bucket-based format; relist required", s)
	}

	wm, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return rv, fmt.Errorf("invalid RV %q: bad watermark: %w", s, err)
	}
	rv.Watermark = wm

	return rv, nil
}
