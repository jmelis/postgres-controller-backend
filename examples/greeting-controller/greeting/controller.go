package greeting

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type GreetingReconciler struct {
	client.Client
}

func (r *GreetingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var greeting Greeting
	if err := r.Get(ctx, req.NamespacedName, &greeting); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if greeting.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	prefix := r.getPrefix(ctx, greeting.Namespace)
	message := fmt.Sprintf("%s, %s!", prefix, greeting.Spec.Name)
	cardName := greeting.Name + "-card"

	card := &GreetingCard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cardName,
			Namespace: greeting.Namespace,
		},
	}
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, card, func() error {
		card.Spec = GreetingCardSpec{
			GreetingName: greeting.Name,
			Message:      message,
		}
		return controllerutil.SetControllerReference(&greeting, card, Scheme)
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("create/update GreetingCard: %w", err)
	}
	logger.Info("GreetingCard reconciled", "name", cardName, "result", result)

	greeting.Status = GreetingStatus{
		Message: message,
		Phase:   "Ready",
		CardRef: cardName,
	}
	if err := r.Status().Update(ctx, &greeting); err != nil {
		return ctrl.Result{}, fmt.Errorf("update Greeting status: %w", err)
	}
	logger.Info("Greeting reconciled", "name", greeting.Name, "message", message)

	return ctrl.Result{}, nil
}

func (r *GreetingReconciler) getPrefix(ctx context.Context, namespace string) string {
	var policies GreetingPolicyList
	if err := r.List(ctx, &policies, client.InNamespace(namespace)); err != nil {
		return "Hello"
	}
	if len(policies.Items) > 0 && policies.Items[0].Spec.Prefix != "" {
		return policies.Items[0].Spec.Prefix
	}
	return "Hello"
}

func (r *GreetingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return r.SetupWithManagerNamed(mgr, "greeting")
}

func (r *GreetingReconciler) SetupWithManagerNamed(mgr ctrl.Manager, name string) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&Greeting{}).
		Owns(&GreetingCard{}).
		Watches(&GreetingPolicy{}, handler.EnqueueRequestsFromMapFunc(r.policyToGreetings)).
		Complete(r)
}

func (r *GreetingReconciler) policyToGreetings(ctx context.Context, obj client.Object) []reconcile.Request {
	var greetings GreetingList
	if err := r.List(ctx, &greetings, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, len(greetings.Items))
	for i, g := range greetings.Items {
		requests[i] = reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: g.Namespace,
				Name:      g.Name,
			},
		}
	}
	return requests
}
