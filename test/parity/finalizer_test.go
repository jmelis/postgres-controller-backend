package parity_test

import (
	"context"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/examples/greeting-controller/greeting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	testFinalizer = "parity.test/hold"
	pollInterval  = 100 * time.Millisecond
)

func TestParity_FinalizerBlocksDeletion(t *testing.T) {
	runOnBothBackends(t, func(t *testing.T, b *Backend) {
		ctx := context.Background()

		g := &greeting.Greeting{
			ObjectMeta: metav1.ObjectMeta{Name: "holdme", Namespace: b.Namespace},
			Spec:       greeting.GreetingSpec{Name: "Hold"},
		}
		require.NoError(t, b.Client.Create(ctx, g))

		key := types.NamespacedName{Namespace: b.Namespace, Name: "holdme"}
		eventuallyGet(t, b.Client, key, g, reconcileTimeout)

		// Add finalizer
		controllerutil.AddFinalizer(g, testFinalizer)
		require.NoError(t, b.Client.Update(ctx, g))

		// Delete — object should still be visible with DeletionTimestamp
		require.NoError(t, b.Client.Delete(ctx, g))

		dying := &greeting.Greeting{}
		eventuallyCondition(t, b.Client, key, dying, reconcileTimeout,
			"deletion timestamp set", func() bool { return dying.DeletionTimestamp != nil })
		assert.Contains(t, dying.Finalizers, testFinalizer)

		// Remove finalizer
		controllerutil.RemoveFinalizer(dying, testFinalizer)
		require.NoError(t, b.Client.Update(ctx, dying))

		// Now it should be truly gone
		require.Eventually(t, func() bool {
			err := b.Client.Get(ctx, key, &greeting.Greeting{})
			return errors.IsNotFound(err)
		}, reconcileTimeout, pollInterval, "object should be NotFound after finalizer removal")
	})
}

func TestParity_DyingObjectInList(t *testing.T) {
	runOnBothBackends(t, func(t *testing.T, b *Backend) {
		ctx := context.Background()

		// Create two greetings
		withFin := &greeting.Greeting{
			ObjectMeta: metav1.ObjectMeta{Name: "with-fin", Namespace: b.Namespace},
			Spec:       greeting.GreetingSpec{Name: "WithFin"},
		}
		require.NoError(t, b.Client.Create(ctx, withFin))

		withoutFin := &greeting.Greeting{
			ObjectMeta: metav1.ObjectMeta{Name: "without-fin", Namespace: b.Namespace},
			Spec:       greeting.GreetingSpec{Name: "WithoutFin"},
		}
		require.NoError(t, b.Client.Create(ctx, withoutFin))

		// Add finalizer to one
		key := types.NamespacedName{Namespace: b.Namespace, Name: "with-fin"}
		eventuallyGet(t, b.Client, key, withFin, reconcileTimeout)
		controllerutil.AddFinalizer(withFin, testFinalizer)
		require.NoError(t, b.Client.Update(ctx, withFin))

		// Re-read withoutFin to get its current resource version
		eventuallyGet(t, b.Client, types.NamespacedName{Namespace: b.Namespace, Name: "without-fin"}, withoutFin, reconcileTimeout)

		// Delete both
		require.NoError(t, b.Client.Delete(ctx, withFin))
		require.NoError(t, b.Client.Delete(ctx, withoutFin))

		// List should include the dying object (with finalizer) but not the fully deleted one
		require.Eventually(t, func() bool {
			list := &greeting.GreetingList{}
			if err := b.Client.List(ctx, list, client.InNamespace(b.Namespace)); err != nil {
				return false
			}
			names := make(map[string]bool)
			for _, item := range list.Items {
				names[item.Name] = true
			}
			return names["with-fin"] && !names["without-fin"]
		}, reconcileTimeout, pollInterval, "list should contain dying object but not fully deleted one")

		// Cleanup: remove finalizer so the dying object can be garbage collected
		fetched := &greeting.Greeting{}
		require.NoError(t, b.Client.Get(ctx, key, fetched))
		controllerutil.RemoveFinalizer(fetched, testFinalizer)
		require.NoError(t, b.Client.Update(ctx, fetched))
	})
}
