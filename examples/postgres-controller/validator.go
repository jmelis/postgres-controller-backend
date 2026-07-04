package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	structuraldefaulting "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/yaml"
)

//go:embed crd/*.yaml
var crdFS embed.FS

type Validator struct {
	specs    map[string]*structuralschema.Structural
	statuses map[string]*structuralschema.Structural
}

func NewValidator() (*Validator, error) {
	v := &Validator{
		specs:    make(map[string]*structuralschema.Structural),
		statuses: make(map[string]*structuralschema.Structural),
	}

	entries, err := crdFS.ReadDir("crd")
	if err != nil {
		return nil, fmt.Errorf("read embedded crd dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		data, err := crdFS.ReadFile("crd/" + entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		if err := v.loadCRD(data); err != nil {
			return nil, fmt.Errorf("load %s: %w", entry.Name(), err)
		}
	}

	return v, nil
}

func (v *Validator) loadCRD(data []byte) error {
	var crdv1 apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crdv1); err != nil {
		return fmt.Errorf("unmarshal CRD: %w", err)
	}

	if len(crdv1.Spec.Versions) == 0 {
		return fmt.Errorf("CRD %s has no versions", crdv1.Name)
	}

	ver := crdv1.Spec.Versions[0]
	if ver.Schema == nil || ver.Schema.OpenAPIV3Schema == nil {
		return fmt.Errorf("CRD %s version %s has no schema", crdv1.Name, ver.Name)
	}

	// Convert v1 schema to internal representation.
	var internalSchema apiextensions.JSONSchemaProps
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(
		ver.Schema.OpenAPIV3Schema, &internalSchema, nil,
	); err != nil {
		return fmt.Errorf("convert schema: %w", err)
	}

	// Build structural schema.
	structural, err := structuralschema.NewStructural(&internalSchema)
	if err != nil {
		return fmt.Errorf("build structural schema: %w", err)
	}
	structuraldefaulting.PruneDefaults(structural)

	gvk := fmt.Sprintf("%s/%s/%s", crdv1.Spec.Group, ver.Name, crdv1.Spec.Names.Kind)

	if specProp, ok := structural.Properties["spec"]; ok {
		v.specs[gvk] = &specProp
	}
	if statusProp, ok := structural.Properties["status"]; ok {
		v.statuses[gvk] = &statusProp
	}

	return nil
}

func (v *Validator) ValidateSpec(gvk string, specJSON json.RawMessage) error {
	s, ok := v.specs[gvk]
	if !ok {
		return fmt.Errorf("no spec schema registered for %s", gvk)
	}

	var obj interface{}
	if err := json.Unmarshal(specJSON, &obj); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	errs := apiservervalidation.ValidateCustomResource(field.NewPath("spec"), obj, apiservervalidation.NewSchemaValidatorFromOpenAPI(s.ToKubeOpenAPI()))
	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %s", errs.ToAggregate().Error())
	}
	return nil
}

func (v *Validator) ValidateStatus(gvk string, statusJSON json.RawMessage) error {
	s, ok := v.statuses[gvk]
	if !ok {
		return nil // no status schema = no validation
	}

	var obj interface{}
	if err := json.Unmarshal(statusJSON, &obj); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	errs := apiservervalidation.ValidateCustomResource(field.NewPath("status"), obj, apiservervalidation.NewSchemaValidatorFromOpenAPI(s.ToKubeOpenAPI()))
	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %s", errs.ToAggregate().Error())
	}
	return nil
}
