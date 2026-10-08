package api

import (
	"fmt"

	"github.com/donnyhardyanto/dxlib/errors"
)

// The JSON Schema annotations a parameter carries: title, default, readOnly
// and writeOnly. Unlike the bounds they promise no check, so they are carried
// both ways and never enforced: Validate ignores them, a default is not filled
// in, and a writeOnly value is not stripped from a response. What is refused
// is only what no reader could make sense of: a default that is not a value of
// the parameter's own JSON type, a null or non-scalar default, and readOnly
// together with writeOnly. Below a parameter (array items, a map's values, a
// body object) they are read and dropped on binding, as description is.

// openAPIAnnotationsFromSchema carries a schema's annotations onto the
// parameter read from it, once its dxlib type is known.
func openAPIAnnotationsFromSchema(s *DXOpenAPISchema, p *DXAPIEndPointParameter, pointer string) error {
	p.Title, p.ReadOnly, p.WriteOnly = s.Title, s.ReadOnly, s.WriteOnly
	if s.Default != nil {
		v := *s.Default
		p.Default = &v
	}
	if err := p.checkAnnotations(openAPITypeTable[p.Type].jsonType); err != nil {
		return errors.Wrapf(err, "OPENAPI_AT:%s", pointer)
	}
	return nil
}

// openAPIAnnotationsToSchema writes a declared parameter's annotations into
// its schema, after checking them as the reader would.
func openAPIAnnotationsToSchema(p *DXAPIEndPointParameter, jsonType string, s *DXOpenAPISchema) error {
	if err := p.checkAnnotations(jsonType); err != nil {
		return err
	}
	s.Title, s.ReadOnly, s.WriteOnly = p.Title, p.ReadOnly, p.WriteOnly
	if p.Default != nil {
		v := *p.Default
		s.Default = &v
	}
	return nil
}

// checkAnnotations refuses the annotations no reader could make sense of.
func (p *DXAPIEndPointParameter) checkAnnotations(jsonType string) error {
	if p.ReadOnly && p.WriteOnly {
		return errors.Errorf("OPENAPI_READ_ONLY_AND_WRITE_ONLY:%s", p.NameId)
	}
	if p.Default == nil {
		return nil
	}
	switch (*p.Default).(type) {
	case string, bool, int, int32, int64, float32, float64:
	case nil:
		return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:default-null:%s", p.NameId)
	default:
		return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:default-%s:%s", fmt.Sprintf("%T", *p.Default), p.NameId)
	}
	if !openAPIScalarFits(*p.Default, jsonType) {
		return errors.Errorf("OPENAPI_DEFAULT_OF_ANOTHER_TYPE:%v(%T):ON_%s:%s", *p.Default, *p.Default, jsonType, p.NameId)
	}
	return nil
}
