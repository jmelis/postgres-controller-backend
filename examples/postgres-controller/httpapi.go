package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/jmelis/postgres-controller-backend/pkg/crbridge"
)

// apiResource is the Kubernetes-like JSON representation for HTTP responses.
type apiResource struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   apiMetadata       `json:"metadata"`
	Spec       json.RawMessage   `json:"spec,omitempty"`
	Status     json.RawMessage   `json:"status,omitempty"`
}

type apiMetadata struct {
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
	UID             string `json:"uid,omitempty"`
}

type APIServer struct {
	clients   map[string]*crbridge.Client
	lws       map[string]*crbridge.ListerWatcher
	validator *Validator
	logger    *slog.Logger
}

func NewAPIServer(
	clients map[string]*crbridge.Client,
	lws map[string]*crbridge.ListerWatcher,
	validator *Validator,
	logger *slog.Logger,
) *APIServer {
	return &APIServer{
		clients:   clients,
		lws:       lws,
		validator: validator,
		logger:    logger,
	}
}

func (s *APIServer) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /namespaces/{ns}/greetings", s.createResource(gvkGreeting, "Greeting"))
	mux.HandleFunc("GET /namespaces/{ns}/greetings/{name}", s.getResource(gvkGreeting, "Greeting"))
	mux.HandleFunc("GET /namespaces/{ns}/greetings", s.listResources(gvkGreeting, "Greeting"))

	mux.HandleFunc("POST /namespaces/{ns}/greetingcards", s.createResource(gvkGreetingCard, "GreetingCard"))
	mux.HandleFunc("GET /namespaces/{ns}/greetingcards/{name}", s.getResource(gvkGreetingCard, "GreetingCard"))
	mux.HandleFunc("GET /namespaces/{ns}/greetingcards", s.listResources(gvkGreetingCard, "GreetingCard"))

	mux.HandleFunc("POST /namespaces/{ns}/greetingpolicies", s.createResource(gvkGreetingPolicy, "GreetingPolicy"))
	mux.HandleFunc("GET /namespaces/{ns}/greetingpolicies/{name}", s.getResource(gvkGreetingPolicy, "GreetingPolicy"))
	mux.HandleFunc("GET /namespaces/{ns}/greetingpolicies", s.listResources(gvkGreetingPolicy, "GreetingPolicy"))
	mux.HandleFunc("PUT /namespaces/{ns}/greetingpolicies/{name}", s.updateResource(gvkGreetingPolicy, "GreetingPolicy"))

	return mux
}

func (s *APIServer) createResource(gvk, kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ns := r.PathValue("ns")

		var body apiResource
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
			return
		}

		if body.Metadata.Name == "" {
			http.Error(w, "metadata.name is required", http.StatusBadRequest)
			return
		}

		if err := s.validator.ValidateSpec(gvk, body.Spec); err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}

		client := s.clients[gvk]
		status := body.Status
		if status == nil {
			status = json.RawMessage(`{}`)
		}

		_, err := client.Create(r.Context(), ns, body.Metadata.Name, body.Spec, status, json.RawMessage(`{}`))
		if err != nil {
			s.writeError(w, err)
			return
		}

		obj, err := client.Get(r.Context(), ns, body.Metadata.Name)
		if err != nil {
			s.writeError(w, err)
			return
		}

		s.writeObject(w, http.StatusCreated, obj, kind)
	}
}

func (s *APIServer) getResource(gvk, kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ns := r.PathValue("ns")
		name := r.PathValue("name")

		client := s.clients[gvk]
		obj, err := client.Get(r.Context(), ns, name)
		if err != nil {
			s.writeError(w, err)
			return
		}

		s.writeObject(w, http.StatusOK, obj, kind)
	}
}

func (s *APIServer) listResources(gvk, kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ns := r.PathValue("ns")
		_ = ns // listing is GVK-scoped across all namespaces for now

		lw := s.lws[gvk]
		result, err := lw.List(context.Background())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		items := make([]apiResource, 0, len(result.Objects))
		for _, obj := range result.Objects {
			if obj.Deleted {
				continue
			}
			items = append(items, objectToAPI(obj, kind))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"apiVersion": apiVersion,
			"kind":       kind + "List",
			"items":      items,
		})
	}
}

func (s *APIServer) updateResource(gvk, kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ns := r.PathValue("ns")
		name := r.PathValue("name")

		var body apiResource
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
			return
		}

		if err := s.validator.ValidateSpec(gvk, body.Spec); err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}

		client := s.clients[gvk]

		existing, err := client.Get(r.Context(), ns, name)
		if err != nil {
			s.writeError(w, err)
			return
		}

		existing.Spec = body.Spec
		updated, err := client.Update(r.Context(), existing)
		if err != nil {
			s.writeError(w, err)
			return
		}

		// Re-read to get full object with spec/status.
		full, err := client.Get(r.Context(), ns, name)
		if err != nil {
			s.writeError(w, err)
			return
		}
		full.ResourceVersion = updated.ResourceVersion

		s.writeObject(w, http.StatusOK, full, kind)
	}
}

func (s *APIServer) writeObject(w http.ResponseWriter, status int, obj *crbridge.Object, kind string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(objectToAPI(obj, kind))
}

func (s *APIServer) writeError(w http.ResponseWriter, err error) {
	switch {
	case err == crbridge.ErrNotFound:
		http.Error(w, "not found", http.StatusNotFound)
	case err == crbridge.ErrAlreadyExists:
		http.Error(w, "already exists", http.StatusConflict)
	case err == crbridge.ErrConflict:
		http.Error(w, "conflict: resource version mismatch", http.StatusConflict)
	case err == crbridge.ErrFenced:
		http.Error(w, "fenced: lease not held", http.StatusServiceUnavailable)
	default:
		s.logger.Error("internal error", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func objectToAPI(obj *crbridge.Object, kind string) apiResource {
	spec := obj.Spec
	if spec == nil {
		spec = json.RawMessage(`{}`)
	}
	status := obj.Status
	if status == nil {
		status = json.RawMessage(`{}`)
	}
	return apiResource{
		APIVersion: apiVersion,
		Kind:       kind,
		Metadata: apiMetadata{
			Name:            obj.Name,
			Namespace:       obj.Namespace,
			ResourceVersion: obj.ResourceVersion,
			UID:             obj.UID.String(),
		},
		Spec:   spec,
		Status: status,
	}
}
