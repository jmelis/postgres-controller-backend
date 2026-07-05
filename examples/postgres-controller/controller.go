package main

import (
	"context"
	"fmt"

	"github.com/jmelis/postgres-controller-backend/pkg/crbridge"
)

// GreetingReconciler reconciles Greeting resources by generating GreetingCards
// with a message prefix determined by the GreetingPolicy in the same namespace.
type GreetingReconciler struct {
	Greetings *crbridge.TypedClient[GreetingSpec, GreetingStatus]
	Cards     *crbridge.TypedClient[GreetingCardSpec, GreetingCardStatus]
	Policies  *crbridge.TypedClient[GreetingPolicySpec, GreetingPolicyStatus]
}

func (r *GreetingReconciler) Reconcile(ctx context.Context, greeting *crbridge.TypedObject[GreetingSpec, GreetingStatus]) (crbridge.Result, error) {
	prefix := r.getPrefix(ctx, greeting.Namespace)
	message := fmt.Sprintf("%s, %s!", prefix, greeting.Spec.Name)
	cardName := greeting.Name + "-card"

	if err := r.ensureCard(ctx, greeting.Namespace, cardName, greeting.Name, message); err != nil {
		return crbridge.Result{}, fmt.Errorf("ensure GreetingCard: %w", err)
	}

	if greeting.Status.Message != message || greeting.Status.Phase != "Ready" || greeting.Status.CardRef != cardName {
		newStatus := GreetingStatus{
			Message: message,
			Phase:   "Ready",
			CardRef: cardName,
		}
		if _, err := r.Greetings.Status().Update(ctx, greeting, newStatus); err != nil {
			return crbridge.Result{}, fmt.Errorf("update Greeting status: %w", err)
		}
	}

	return crbridge.Result{}, nil
}

func (r *GreetingReconciler) getPrefix(ctx context.Context, namespace string) string {
	result, err := r.Policies.List(ctx)
	if err != nil {
		return "Hello"
	}
	for _, p := range result.Objects {
		if !p.Deleted && p.Namespace == namespace && p.Spec.Prefix != "" {
			return p.Spec.Prefix
		}
	}
	return "Hello"
}

func (r *GreetingReconciler) ensureCard(ctx context.Context, namespace, cardName, greetingName, message string) error {
	existing, err := r.Cards.Get(ctx, namespace, cardName)
	if err == crbridge.ErrNotFound {
		_, err := r.Cards.Create(ctx, namespace, cardName, GreetingCardSpec{
			GreetingName: greetingName,
			Message:      message,
		})
		return err
	}
	if err != nil {
		return err
	}

	if existing.Spec.Message == message {
		return nil
	}

	existing.Spec = GreetingCardSpec{
		GreetingName: greetingName,
		Message:      message,
	}
	_, err = r.Cards.Update(ctx, existing)
	return err
}

// policyToGreetings maps a GreetingPolicy change to Requests for all
// Greetings in the same namespace, so they get re-reconciled with the
// updated prefix.
func (r *GreetingReconciler) policyToGreetings(ctx context.Context, obj *crbridge.Object) []crbridge.Request {
	result, err := r.Greetings.List(ctx)
	if err != nil {
		return nil
	}
	var requests []crbridge.Request
	for _, g := range result.Objects {
		if !g.Deleted && g.Namespace == obj.Namespace {
			requests = append(requests, crbridge.Request{
				Namespace: g.Namespace,
				Name:      g.Name,
			})
		}
	}
	return requests
}
