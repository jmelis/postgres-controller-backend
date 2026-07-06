package pgruntime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func groupResource(gvk schema.GroupVersionKind) schema.GroupResource {
	return schema.GroupResource{
		Group:    gvk.Group,
		Resource: strings.ToLower(gvk.Kind),
	}
}

func mapGetError(err error, gvk schema.GroupVersionKind, name string) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return apierrors.NewNotFound(groupResource(gvk), name)
	}
	return err
}

func mapWriteError(ctx context.Context, w *writer.Writer, err error, gvk schema.GroupVersionKind, name string, seq int64) (*model.Resource, error) {
	if errors.Is(err, writer.ErrAlreadyExists) {
		return nil, apierrors.NewAlreadyExists(groupResource(gvk), name)
	}
	if errors.Is(err, writer.ErrConflict) {
		return nil, apierrors.NewConflict(groupResource(gvk), name, err)
	}
	if errors.Is(err, writer.ErrFenceViolation) {
		return nil, apierrors.NewConflict(groupResource(gvk), name, err)
	}

	var ace *writer.AmbiguousCommitError
	if errors.As(err, &ace) {
		r, rbErr := w.ReadBack(ctx, ace.GVK, ace.Namespace, ace.Name, ace.Seq)
		if rbErr != nil {
			return nil, fmt.Errorf("ambiguous commit + read-back failed: %w (original: %v)", rbErr, err)
		}
		if r == nil {
			return nil, apierrors.NewConflict(groupResource(gvk), name, err)
		}
		return r, nil
	}

	return nil, err
}
