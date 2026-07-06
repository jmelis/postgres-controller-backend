package parity_test

import (
	"context"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/examples/greeting-controller/greeting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const reconcileTimeout = 10 * time.Second

func TestParity_FullReconcile(t *testing.T) {
	runOnBothBackends(t, func(t *testing.T, b *Backend) {
		ctx := context.Background()

		g := &greeting.Greeting{
			ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: b.Namespace},
			Spec:       greeting.GreetingSpec{Name: "Alice"},
		}
		require.NoError(t, b.Client.Create(ctx, g))

		// Wait for reconciler to create the GreetingCard
		card := &greeting.GreetingCard{}
		cardKey := types.NamespacedName{Namespace: b.Namespace, Name: "alice-card"}
		eventuallyGet(t, b.Client, cardKey, card, reconcileTimeout)
		assert.Equal(t, "Hello, Alice!", card.Spec.Message)
		assert.Equal(t, "alice", card.Spec.GreetingName)

		// Wait for status to be set
		fetched := &greeting.Greeting{}
		eventuallyCondition(t, b.Client, types.NamespacedName{Namespace: b.Namespace, Name: "alice"}, fetched, reconcileTimeout,
			"status Ready", func() bool { return fetched.Status.Phase == "Ready" })
		assert.Equal(t, "Hello, Alice!", fetched.Status.Message)
		assert.Equal(t, "alice-card", fetched.Status.CardRef)
	})
}

func TestParity_PolicyPrefix(t *testing.T) {
	runOnBothBackends(t, func(t *testing.T, b *Backend) {
		ctx := context.Background()

		policy := &greeting.GreetingPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "spanish", Namespace: b.Namespace},
			Spec:       greeting.GreetingPolicySpec{Prefix: "Hola"},
		}
		require.NoError(t, b.Client.Create(ctx, policy))

		g := &greeting.Greeting{
			ObjectMeta: metav1.ObjectMeta{Name: "bob", Namespace: b.Namespace},
			Spec:       greeting.GreetingSpec{Name: "Bob"},
		}
		require.NoError(t, b.Client.Create(ctx, g))

		fetched := &greeting.Greeting{}
		eventuallyCondition(t, b.Client, types.NamespacedName{Namespace: b.Namespace, Name: "bob"}, fetched, reconcileTimeout,
			"status Ready", func() bool { return fetched.Status.Phase == "Ready" })
		assert.Equal(t, "Hola, Bob!", fetched.Status.Message)
	})
}

func TestParity_UpdateReReconciles(t *testing.T) {
	runOnBothBackends(t, func(t *testing.T, b *Backend) {
		ctx := context.Background()

		g := &greeting.Greeting{
			ObjectMeta: metav1.ObjectMeta{Name: "charlie", Namespace: b.Namespace},
			Spec:       greeting.GreetingSpec{Name: "Charlie"},
		}
		require.NoError(t, b.Client.Create(ctx, g))

		fetched := &greeting.Greeting{}
		eventuallyCondition(t, b.Client, types.NamespacedName{Namespace: b.Namespace, Name: "charlie"}, fetched, reconcileTimeout,
			"status Ready", func() bool { return fetched.Status.Phase == "Ready" })
		assert.Equal(t, "Hello, Charlie!", fetched.Status.Message)

		// Update the spec name
		fetched.Spec.Name = "Charles"
		require.NoError(t, b.Client.Update(ctx, fetched))

		// Wait for status to reflect the new name
		eventuallyCondition(t, b.Client, types.NamespacedName{Namespace: b.Namespace, Name: "charlie"}, fetched, reconcileTimeout,
			"status updated", func() bool { return fetched.Status.Message == "Hello, Charles!" })

		card := &greeting.GreetingCard{}
		eventuallyCondition(t, b.Client, types.NamespacedName{Namespace: b.Namespace, Name: "charlie-card"}, card, reconcileTimeout,
			"card updated", func() bool { return card.Spec.Message == "Hello, Charles!" })
	})
}

func TestParity_OwnerReferences(t *testing.T) {
	runOnBothBackends(t, func(t *testing.T, b *Backend) {
		ctx := context.Background()

		g := &greeting.Greeting{
			ObjectMeta: metav1.ObjectMeta{Name: "dana", Namespace: b.Namespace},
			Spec:       greeting.GreetingSpec{Name: "Dana"},
		}
		require.NoError(t, b.Client.Create(ctx, g))

		// Re-read to get the UID assigned by the backend
		gKey := types.NamespacedName{Namespace: b.Namespace, Name: "dana"}
		eventuallyGet(t, b.Client, gKey, g, reconcileTimeout)

		card := &greeting.GreetingCard{}
		cardKey := types.NamespacedName{Namespace: b.Namespace, Name: "dana-card"}
		eventuallyGet(t, b.Client, cardKey, card, reconcileTimeout)

		require.Len(t, card.OwnerReferences, 1)
		ownerRef := card.OwnerReferences[0]
		assert.Equal(t, g.UID, ownerRef.UID)
		assert.Equal(t, "Greeting", ownerRef.Kind)
		assert.Equal(t, "greeting.example.com/v1alpha1", ownerRef.APIVersion)
		assert.NotNil(t, ownerRef.Controller)
		assert.True(t, *ownerRef.Controller)
	})
}

func TestParity_StatusSubresource(t *testing.T) {
	runOnBothBackends(t, func(t *testing.T, b *Backend) {
		ctx := context.Background()

		g := &greeting.Greeting{
			ObjectMeta: metav1.ObjectMeta{Name: "eve", Namespace: b.Namespace},
			Spec:       greeting.GreetingSpec{Name: "Eve"},
		}
		require.NoError(t, b.Client.Create(ctx, g))

		key := types.NamespacedName{Namespace: b.Namespace, Name: "eve"}
		fetched := &greeting.Greeting{}
		eventuallyCondition(t, b.Client, key, fetched, reconcileTimeout,
			"status Ready", func() bool { return fetched.Status.Phase == "Ready" })

		// Verify spec was not clobbered by the status update
		assert.Equal(t, "Eve", fetched.Spec.Name)
		assert.Equal(t, "Hello, Eve!", fetched.Status.Message)
		assert.Equal(t, "eve-card", fetched.Status.CardRef)

		// Verify the object is listable
		list := &greeting.GreetingList{}
		require.NoError(t, b.Client.List(ctx, list, client.InNamespace(b.Namespace)))
		assert.GreaterOrEqual(t, len(list.Items), 1)
	})
}
