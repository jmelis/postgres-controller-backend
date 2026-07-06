package parity_test

import (
	"context"
	"testing"

	"github.com/jmelis/postgres-controller-backend/examples/greeting-controller/greeting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestParity_GetNotFound(t *testing.T) {
	runOnBothBackends(t, func(t *testing.T, b *Backend) {
		g := &greeting.Greeting{}
		err := b.Client.Get(context.Background(),
			types.NamespacedName{Namespace: b.Namespace, Name: "nonexistent"}, g)
		assert.True(t, errors.IsNotFound(err), "expected NotFound, got: %v", err)
	})
}

func TestParity_CreateAlreadyExists(t *testing.T) {
	runOnBothBackends(t, func(t *testing.T, b *Backend) {
		ctx := context.Background()

		g := &greeting.Greeting{
			ObjectMeta: metav1.ObjectMeta{Name: "duplicate", Namespace: b.Namespace},
			Spec:       greeting.GreetingSpec{Name: "Dup"},
		}
		require.NoError(t, b.Client.Create(ctx, g))

		g2 := &greeting.Greeting{
			ObjectMeta: metav1.ObjectMeta{Name: "duplicate", Namespace: b.Namespace},
			Spec:       greeting.GreetingSpec{Name: "Dup2"},
		}
		err := b.Client.Create(ctx, g2)
		assert.True(t, errors.IsAlreadyExists(err), "expected AlreadyExists, got: %v", err)
	})
}

func TestParity_UpdateConflict(t *testing.T) {
	runOnBothBackends(t, func(t *testing.T, b *Backend) {
		ctx := context.Background()

		g := &greeting.Greeting{
			ObjectMeta: metav1.ObjectMeta{Name: "conflict", Namespace: b.Namespace},
			Spec:       greeting.GreetingSpec{Name: "Original"},
		}
		require.NoError(t, b.Client.Create(ctx, g))

		key := types.NamespacedName{Namespace: b.Namespace, Name: "conflict"}

		// Get two copies
		copy1 := &greeting.Greeting{}
		eventuallyGet(t, b.Client, key, copy1, reconcileTimeout)

		copy2 := &greeting.Greeting{}
		eventuallyGet(t, b.Client, key, copy2, reconcileTimeout)

		// Update copy1 first
		copy1.Spec.Name = "Updated"
		require.NoError(t, b.Client.Update(ctx, copy1))

		// Update copy2 (stale) — should conflict
		copy2.Spec.Name = "Stale"
		err := b.Client.Update(ctx, copy2)
		assert.True(t, errors.IsConflict(err), "expected Conflict, got: %v", err)
	})
}
