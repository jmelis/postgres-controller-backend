package verifier

import (
	"reflect"
	"testing"

	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
)

// B5 — Verifier state must be O(1), not O(events).
//
// The old seenKeys map recorded every (bucket, seq) pair ever seen, growing
// without bound. After the fix, per-event state is gone: duplicates are caught
// by the hwm monotonicity check (txid <= hwm => I3 violation).
func TestVerifier_NoUnboundedMap(t *testing.T) {
	typ := reflect.TypeOf(Verifier{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.Type.Kind() == reflect.Map &&
			f.Type.Key().Kind() == reflect.String &&
			f.Type.Elem().Kind() == reflect.Bool {
			t.Fatalf("B5: Verifier has unbounded %s field %q — "+
				"should use hwm-based monotonicity check instead of per-key tracking",
				f.Type, f.Name)
		}
	}
}

// TestDuplicateDetection_ViaMonotonicity verifies that duplicate delivery is
// caught by the I3 monotonicity check (txid <= hwm). No per-event map needed.
func TestDuplicateDetection_ViaMonotonicity(t *testing.T) {
	v := &Verifier{
		cfg: Config{
			GVK: "apps/v1/Deployment",
		},
		hwm: 0,
	}

	ev := reader.Event{
		Type: reader.EventAdded,
		Resource: model.Resource{
			GVK:       "apps/v1/Deployment",
			TxidStamp: 1,
		},
	}

	v.checkEvent(ev) // first delivery — clean
	v.checkEvent(ev) // duplicate — txid=1 <= hwm=1

	var found bool
	for _, viol := range v.violations {
		if viol.Invariant == "I2" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("duplicate delivery not detected by hwm monotonicity (I2)")
	}
}
