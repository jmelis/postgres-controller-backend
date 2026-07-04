package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jmelis/postgres-controller-backend/pkg/crbridge"
)

type Controller struct {
	greetingClient *crbridge.Client
	cardClient     *crbridge.Client
	policyClient   *crbridge.Client
	greetingLW     *crbridge.ListerWatcher
	policyLW       *crbridge.ListerWatcher
	cardLW         *crbridge.ListerWatcher
	validator      *Validator
	logger         *slog.Logger

	queue chan string // namespace/name keys to reconcile
}

func NewController(
	greetingClient, cardClient, policyClient *crbridge.Client,
	greetingLW, policyLW, cardLW *crbridge.ListerWatcher,
	validator *Validator,
	logger *slog.Logger,
) *Controller {
	return &Controller{
		greetingClient: greetingClient,
		cardClient:     cardClient,
		policyClient:   policyClient,
		greetingLW:     greetingLW,
		policyLW:       policyLW,
		cardLW:         cardLW,
		validator:      validator,
		logger:         logger,
		queue:          make(chan string, 256),
	}
}

func (c *Controller) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	wg.Add(3)
	go func() { defer wg.Done(); c.watchGreetings(ctx) }()
	go func() { defer wg.Done(); c.watchPolicies(ctx) }()
	go func() { defer wg.Done(); c.reconcileLoop(ctx) }()

	wg.Wait()
	return ctx.Err()
}

func (c *Controller) watchGreetings(ctx context.Context) {
	for ctx.Err() == nil {
		c.logger.Info("listing Greetings")
		result, err := c.greetingLW.List(ctx)
		if err != nil {
			c.logger.Error("list Greetings failed", "err", err)
			return
		}

		for _, obj := range result.Objects {
			if !obj.Deleted {
				c.enqueue(obj.Namespace, obj.Name)
			}
		}

		c.logger.Info("watching Greetings", "rv", result.ResourceVersion)
		wi, err := c.greetingLW.Watch(ctx, result.ResourceVersion)
		if err != nil {
			c.logger.Error("watch Greetings failed", "err", err)
			continue
		}

		for ev := range wi.ResultChan() {
			if ev.Object == nil {
				continue
			}
			c.logger.Info("Greeting event", "type", ev.Type, "name", ev.Object.Name)
			if ev.Type == crbridge.EventAdded || ev.Type == crbridge.EventModified {
				c.enqueue(ev.Object.Namespace, ev.Object.Name)
			}
		}
		c.logger.Info("Greeting watch closed, relisting")
	}
}

func (c *Controller) watchPolicies(ctx context.Context) {
	for ctx.Err() == nil {
		result, err := c.policyLW.List(ctx)
		if err != nil {
			c.logger.Error("list GreetingPolicies failed", "err", err)
			return
		}

		c.logger.Info("watching GreetingPolicies", "rv", result.ResourceVersion)
		wi, err := c.policyLW.Watch(ctx, result.ResourceVersion)
		if err != nil {
			c.logger.Error("watch GreetingPolicies failed", "err", err)
			continue
		}

		for ev := range wi.ResultChan() {
			if ev.Object == nil {
				continue
			}
			c.logger.Info("GreetingPolicy event", "type", ev.Type, "name", ev.Object.Name)
			c.requeueAllGreetings(ctx, ev.Object.Namespace)
		}
		c.logger.Info("GreetingPolicy watch closed, relisting")
	}
}

func (c *Controller) requeueAllGreetings(ctx context.Context, namespace string) {
	result, err := c.greetingLW.List(ctx)
	if err != nil {
		c.logger.Error("list Greetings for requeue failed", "err", err)
		return
	}
	for _, obj := range result.Objects {
		if !obj.Deleted && obj.Namespace == namespace {
			c.enqueue(obj.Namespace, obj.Name)
		}
	}
}

func (c *Controller) enqueue(namespace, name string) {
	key := namespace + "/" + name
	select {
	case c.queue <- key:
	default:
		c.logger.Warn("reconcile queue full, dropping", "key", key)
	}
}

func (c *Controller) reconcileLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case key := <-c.queue:
			ns, name := splitKey(key)
			if err := c.reconcile(ctx, ns, name); err != nil {
				c.logger.Error("reconcile failed", "key", key, "err", err)
				c.enqueue(ns, name) // requeue on error
			}
		}
	}
}

func (c *Controller) reconcile(ctx context.Context, namespace, name string) error {
	greeting, err := c.greetingClient.Get(ctx, namespace, name)
	if err != nil {
		return fmt.Errorf("get Greeting: %w", err)
	}

	// Parse spec.
	var spec struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(greeting.Spec, &spec); err != nil {
		return fmt.Errorf("unmarshal Greeting spec: %w", err)
	}

	// Get prefix from GreetingPolicy.
	prefix := c.getPrefix(ctx, namespace)
	message := fmt.Sprintf("%s, %s!", prefix, spec.Name)
	cardName := name + "-card"

	// Create or update GreetingCard.
	if err := c.ensureCard(ctx, namespace, cardName, name, message); err != nil {
		return fmt.Errorf("ensure GreetingCard: %w", err)
	}

	// Update Greeting status only if it changed.
	var currentStatus struct {
		Message string `json:"message"`
		Phase   string `json:"phase"`
		CardRef string `json:"cardRef"`
	}
	_ = json.Unmarshal(greeting.Status, &currentStatus)

	if currentStatus.Message != message || currentStatus.Phase != "Ready" || currentStatus.CardRef != cardName {
		newStatus, _ := json.Marshal(map[string]string{
			"message": message,
			"phase":   "Ready",
			"cardRef": cardName,
		})

		if err := c.validator.ValidateStatus(gvkGreeting, newStatus); err != nil {
			return fmt.Errorf("validate status: %w", err)
		}

		if _, err := c.greetingClient.Status().Update(greeting, newStatus); err != nil {
			return fmt.Errorf("update Greeting status: %w", err)
		}
	}

	c.logger.Info("Greeting reconciled", "name", name, "message", message)
	return nil
}

func (c *Controller) getPrefix(ctx context.Context, namespace string) string {
	result, err := c.policyLW.List(ctx)
	if err != nil {
		return "Hello"
	}
	for _, obj := range result.Objects {
		if obj.Deleted || obj.Namespace != namespace {
			continue
		}
		var spec struct {
			Prefix string `json:"prefix"`
		}
		if err := json.Unmarshal(obj.Spec, &spec); err == nil && spec.Prefix != "" {
			return spec.Prefix
		}
	}
	return "Hello"
}

func (c *Controller) ensureCard(ctx context.Context, namespace, cardName, greetingName, message string) error {
	cardSpec, _ := json.Marshal(map[string]string{
		"greetingName": greetingName,
		"message":      message,
	})

	if err := c.validator.ValidateSpec(gvkGreetingCard, cardSpec); err != nil {
		return fmt.Errorf("validate GreetingCard spec: %w", err)
	}

	existing, err := c.cardClient.Get(ctx, namespace, cardName)
	if err == crbridge.ErrNotFound {
		_, err := c.cardClient.Create(ctx, namespace, cardName, cardSpec, json.RawMessage(`{}`), json.RawMessage(`{}`))
		return err
	}
	if err != nil {
		return err
	}

	// Update if message changed.
	var existingSpec struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(existing.Spec, &existingSpec); err == nil && existingSpec.Message == message {
		return nil
	}

	existing.Spec = cardSpec
	_, err = c.cardClient.Update(ctx, existing)
	return err
}

func splitKey(key string) (string, string) {
	for i, c := range key {
		if c == '/' {
			return key[:i], key[i+1:]
		}
	}
	return "", key
}
