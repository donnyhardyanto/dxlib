package api

import (
	"fmt"

	"github.com/donnyhardyanto/dxlib/errors"
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// The mapping between dxlib's parameter types and JSON Schema, in both
// directions. Forward (dxlib -> schema) is total over the types dxlib
// declares. Backward (schema -> dxlib) is exact when the schema carries
// x-dxlib-type and an inference from type, format and constraints when it
// does not; an inference that would promise something the runtime does not
// check is refused rather than approximated.

// openAPIJSONSchemaTypes is the JSON Schema type vocabulary. "null" is
// legal only beside another type.
var openAPIJSONSchemaTypes = map[string]bool{
	"string": true, "integer": true, "number": true, "boolean": true, "object": true, "array": true, "null": true,
}

// openAPIKnownFormats is every format the forward mapping writes. A format
// outside this set names a validator dxlib does not have, so it is refused
// at read time rather than carried as a promise nobody keeps.
var openAPIKnownFormats = map[string]bool{
	"int32": true, "int64": true, "float": true, "double": true,
	"date-time": true, "date": true, "time": true, "email": true,
	"byte": true, "binary": true,
}

type openAPITypeMapping struct {
	jsonType         string
	format           string
	minimum          *float64
	exclusiveMinimum *float64
	minLength        *int
}

func openAPIFloat(v float64) *float64 { return &v }
func openAPIInt(v int) *int           { return &v }

// openAPITypeTable is the forward mapping. Where dxlib's validator enforces a
// bound the schema states it: int64p is "integer, minimum 1", not just
// "integer", so a consumer reading the schema learns what the server will do.
var openAPITypeTable = map[dxlibTypes.APIParameterType]openAPITypeMapping{
	dxlibTypes.APIParameterTypeEncryptedBlob: {jsonType: "string", format: "byte"},
	dxlibTypes.APIParameterTypeBlob:          {jsonType: "string", format: "binary"},

	dxlibTypes.APIParameterTypeString:                  {jsonType: "string"},
	dxlibTypes.APIParameterTypeProtectedString:         {jsonType: "string"},
	dxlibTypes.APIParameterTypeProtectedSQLString:      {jsonType: "string"},
	dxlibTypes.APIParameterTypeProtectedNonEmptyString: {jsonType: "string", minLength: openAPIInt(1)},
	dxlibTypes.APIParameterTypeNullableString:          {jsonType: "string"},
	dxlibTypes.APIParameterTypeNonEmptyString:          {jsonType: "string", minLength: openAPIInt(1)},
	dxlibTypes.APIParameterTypeEmail:                   {jsonType: "string", format: "email"},
	dxlibTypes.APIParameterTypePhoneNumber:             {jsonType: "string"},
	dxlibTypes.APIParameterTypeNPWP:                    {jsonType: "string"},

	dxlibTypes.APIParameterTypeInt32:         {jsonType: "integer", format: "int32"},
	dxlibTypes.APIParameterTypeInt32P:        {jsonType: "integer", format: "int32", minimum: openAPIFloat(1)},
	dxlibTypes.APIParameterTypeInt32ZP:       {jsonType: "integer", format: "int32", minimum: openAPIFloat(0)},
	dxlibTypes.APIParameterTypeNullableInt32: {jsonType: "integer", format: "int32"},
	dxlibTypes.APIParameterTypeInt64:         {jsonType: "integer", format: "int64"},
	dxlibTypes.APIParameterTypeInt64P:        {jsonType: "integer", format: "int64", minimum: openAPIFloat(1)},
	dxlibTypes.APIParameterTypeInt64ZP:       {jsonType: "integer", format: "int64", minimum: openAPIFloat(0)},
	dxlibTypes.APIParameterTypeNullableInt64: {jsonType: "integer", format: "int64"},
	dxlibTypes.APIParameterTypeID:            {jsonType: "integer", format: "int64"},

	dxlibTypes.APIParameterTypeFloat32:   {jsonType: "number", format: "float"},
	dxlibTypes.APIParameterTypeFloat32P:  {jsonType: "number", format: "float", exclusiveMinimum: openAPIFloat(0)},
	dxlibTypes.APIParameterTypeFloat32ZP: {jsonType: "number", format: "float", minimum: openAPIFloat(0)},
	dxlibTypes.APIParameterTypeFloat64:   {jsonType: "number", format: "double"},
	dxlibTypes.APIParameterTypeFloat64P:  {jsonType: "number", format: "double", exclusiveMinimum: openAPIFloat(0)},
	dxlibTypes.APIParameterTypeFloat64ZP: {jsonType: "number", format: "double", minimum: openAPIFloat(0)},

	// Money crosses the wire as a decimal string; there is no registered
	// format for that, so the string type stands and x-dxlib-type says money.
	dxlibTypes.APIParameterTypeMoney: {jsonType: "string"},

	dxlibTypes.APIParameterTypeBoolean: {jsonType: "boolean"},

	dxlibTypes.APIParameterTypeISO8601: {jsonType: "string", format: "date-time"},
	dxlibTypes.APIParameterTypeDate:    {jsonType: "string", format: "date"},
	dxlibTypes.APIParameterTypeTime:    {jsonType: "string", format: "time"},

	dxlibTypes.APIParameterTypeJSON:            {jsonType: "object"},
	dxlibTypes.APIParameterTypeJSONPassthrough: {jsonType: "object"},

	dxlibTypes.APIParameterTypeArray:             {jsonType: "array"},
	dxlibTypes.APIParameterTypeArrayString:       {jsonType: "array"},
	dxlibTypes.APIParameterTypeArrayInt64:        {jsonType: "array"},
	dxlibTypes.APIParameterTypeArrayJSONTemplate: {jsonType: "array"},

	dxlibTypes.APIParameterTypeMapStringString: {jsonType: "object"},
}

// openAPISchemaFromParameter is the forward mapping for one declared
// parameter, children included. "null" joins the type list exactly when
// IsNullable is set, so that flag survives a round trip on its own; the dxlib
// type itself travels in x-dxlib-type.
func openAPISchemaFromParameter(p *DXAPIEndPointParameter) (*DXOpenAPISchema, error) {
	m, ok := openAPITypeTable[p.Type]
	if !ok {
		return nil, errors.Errorf("OPENAPI_UNMAPPED_PARAMETER_TYPE:%q:%s", string(p.Type), p.NameId)
	}
	s := &DXOpenAPISchema{
		Type:             DXOpenAPISchemaType{m.jsonType},
		Format:           m.format,
		Description:      p.Description,
		Minimum:          m.minimum,
		ExclusiveMinimum: m.exclusiveMinimum,
		MinLength:        m.minLength,
		DXLibType:        string(p.Type),
	}
	if p.IsNullable {
		s.Type = append(s.Type, "null")
	}
	if len(p.Enum) > 0 {
		s.Enum = append([]any{}, p.Enum...)
	}
	switch p.Type {
	case dxlibTypes.APIParameterTypeJSON:
		if len(p.Children) > 0 {
			props, required, err := openAPIPropertiesFromParameters(p.Children)
			if err != nil {
				return nil, errors.Wrapf(err, "OPENAPI_CHILD_OF:%s", p.NameId)
			}
			s.Properties, s.Required = props, required
		}
	case dxlibTypes.APIParameterTypeArrayJSONTemplate:
		item := &DXOpenAPISchema{Type: DXOpenAPISchemaType{"object"}}
		if len(p.Children) > 0 {
			props, required, err := openAPIPropertiesFromParameters(p.Children)
			if err != nil {
				return nil, errors.Wrapf(err, "OPENAPI_CHILD_OF:%s", p.NameId)
			}
			item.Properties, item.Required = props, required
		}
		s.Items = item
	case dxlibTypes.APIParameterTypeArrayString:
		s.Items = &DXOpenAPISchema{Type: DXOpenAPISchemaType{"string"}}
	case dxlibTypes.APIParameterTypeArrayInt64:
		s.Items = &DXOpenAPISchema{Type: DXOpenAPISchemaType{"integer"}, Format: "int64"}
	case dxlibTypes.APIParameterTypeMapStringString:
		s.AdditionalProperties = &DXOpenAPISchema{Type: DXOpenAPISchemaType{"string"}}
	}
	return s, nil
}

// openAPIPropertiesFromParameters builds an object schema's properties and
// required list from a parameter list, in declaration order.
func openAPIPropertiesFromParameters(parameters []DXAPIEndPointParameter) (*DXOpenAPIOrderedMap[*DXOpenAPISchema], []string, error) {
	props := NewDXOpenAPIOrderedMap[*DXOpenAPISchema]()
	var required []string
	for i := range parameters {
		p := &parameters[i]
		if _, dup := props.Get(p.NameId); dup {
			return nil, nil, errors.Errorf("OPENAPI_DUPLICATE_PARAMETER:%s", p.NameId)
		}
		s, err := openAPISchemaFromParameter(p)
		if err != nil {
			return nil, nil, err
		}
		props.Set(p.NameId, s)
		if p.IsMustExist {
			required = append(required, p.NameId)
		}
	}
	return props, required, nil
}

// openAPISchemaResolver looks up local $ref targets. The reader has already
// checked that every $ref points at an existing components/schemas entry;
// the resolver's own job is to stop a schema that references itself from
// recursing forever.
type openAPISchemaResolver struct {
	components *DXOpenAPIComponents
	inProgress map[string]bool
}

func (r *openAPISchemaResolver) resolve(s *DXOpenAPISchema, pointer string) (*DXOpenAPISchema, func(), error) {
	if s.Ref == "" {
		return s, func() {}, nil
	}
	name, err := openAPIRefName(s.Ref)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "OPENAPI_REF:%s", pointer)
	}
	if r.inProgress[name] {
		return nil, nil, errors.Errorf("OPENAPI_REF_CYCLE:%s:%s", s.Ref, pointer)
	}
	if r.components == nil || r.components.Schemas == nil {
		return nil, nil, errors.Errorf("OPENAPI_REF_NOT_FOUND:%s:%s", s.Ref, pointer)
	}
	target, ok := r.components.Schemas.Get(name)
	if !ok {
		return nil, nil, errors.Errorf("OPENAPI_REF_NOT_FOUND:%s:%s", s.Ref, pointer)
	}
	if r.inProgress == nil {
		r.inProgress = map[string]bool{}
	}
	r.inProgress[name] = true
	release := func() { delete(r.inProgress, name) }
	// A reference to a reference is followed, and the outer name stays marked
	// until the whole chain is done, which is what makes a cycle through two
	// names visible.
	if target.Ref != "" {
		inner, innerRelease, err := r.resolve(target, pointer)
		if err != nil {
			release()
			return nil, nil, err
		}
		return inner, func() { innerRelease(); release() }, nil
	}
	return target, release, nil
}

const openAPIRefPrefix = "#/components/schemas/"

// openAPIRefName accepts only a local reference into components/schemas.
// Anything else -- another file, a URL, another components section -- is a
// construct this reader does not implement, and is named as such.
func openAPIRefName(ref string) (string, error) {
	if len(ref) > len(openAPIRefPrefix) && ref[:len(openAPIRefPrefix)] == openAPIRefPrefix {
		name := ref[len(openAPIRefPrefix):]
		for _, c := range name {
			if c == '/' || c == '#' {
				return "", errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:$ref:%q:ONLY_%s<name>_IS_SUPPORTED", ref, openAPIRefPrefix)
			}
		}
		return name, nil
	}
	if len(ref) > 0 && ref[0] != '#' {
		return "", errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:remote-$ref:%q:ONLY_LOCAL_%s<name>_IS_SUPPORTED", ref, openAPIRefPrefix)
	}
	return "", errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:$ref:%q:ONLY_%s<name>_IS_SUPPORTED", ref, openAPIRefPrefix)
}

// openAPIParameterFromSchema is the backward mapping for one property.
// pointer is the JSON pointer of the schema, for the error messages.
func openAPIParameterFromSchema(name string, s *DXOpenAPISchema, isMustExist bool, r *openAPISchemaResolver, pointer string) (DXAPIEndPointParameter, error) {
	resolved, release, err := r.resolve(s, pointer)
	if err != nil {
		return DXAPIEndPointParameter{}, err
	}
	defer release()
	s = resolved

	p := DXAPIEndPointParameter{
		NameId:      name,
		Description: s.Description,
		IsMustExist: isMustExist,
		IsNullable:  s.Type.Nullable(),
	}
	if len(s.Enum) > 0 {
		p.Enum = append([]any{}, s.Enum...)
	}

	primary := s.Type.Primary()
	if s.DXLibType != "" {
		t := dxlibTypes.APIParameterType(s.DXLibType)
		m, ok := openAPITypeTable[t]
		if !ok {
			return p, errors.Errorf("OPENAPI_UNKNOWN_X_DXLIB_TYPE:%q:%s", s.DXLibType, pointer)
		}
		if primary != "" && primary != m.jsonType {
			return p, errors.Errorf("OPENAPI_SCHEMA_TYPE_CONTRADICTS_X_DXLIB_TYPE:type=%s:x-dxlib-type=%s(expects %s):%s", primary, s.DXLibType, m.jsonType, pointer)
		}
		if s.Format != "" && m.format != "" && s.Format != m.format {
			return p, errors.Errorf("OPENAPI_SCHEMA_FORMAT_CONTRADICTS_X_DXLIB_TYPE:format=%s:x-dxlib-type=%s(expects %s):%s", s.Format, s.DXLibType, m.format, pointer)
		}
		p.Type = t
	} else {
		t, err := openAPIInferType(s, pointer)
		if err != nil {
			return p, err
		}
		p.Type = t
	}

	switch p.Type {
	case dxlibTypes.APIParameterTypeJSON:
		children, err := openAPIParametersFromProperties(s, r, pointer)
		if err != nil {
			return p, err
		}
		p.Children = children
	case dxlibTypes.APIParameterTypeArrayJSONTemplate:
		if s.Items == nil {
			return p, errors.Errorf("OPENAPI_ARRAY_JSON_TEMPLATE_WITHOUT_ITEMS:%s", pointer)
		}
		items, itemsRelease, err := r.resolve(s.Items, pointer+"/items")
		if err != nil {
			return p, err
		}
		children, err := openAPIParametersFromProperties(items, r, pointer+"/items")
		itemsRelease()
		if err != nil {
			return p, err
		}
		p.Children = children
	}
	return p, nil
}

// openAPIParametersFromProperties converts an object schema's properties to
// a parameter list, in document order, with required-ness from the required
// list.
func openAPIParametersFromProperties(s *DXOpenAPISchema, r *openAPISchemaResolver, pointer string) ([]DXAPIEndPointParameter, error) {
	if s.Properties == nil {
		return nil, nil
	}
	required := map[string]bool{}
	for _, name := range s.Required {
		required[name] = true
	}
	var out []DXAPIEndPointParameter
	for _, name := range s.Properties.Keys() {
		prop, _ := s.Properties.Get(name)
		p, err := openAPIParameterFromSchema(name, prop, required[name], r, pointer+"/properties/"+openAPIPointerEscape(name))
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// openAPIInferType is the backward mapping without x-dxlib-type. The rule is
// that every constraint the schema states must be one dxlib enforces, so the
// document never says more than the server does: minimum 0 or 1 on an integer
// is int64zp or int64p, minimum 5 is refused.
func openAPIInferType(s *DXOpenAPISchema, pointer string) (dxlibTypes.APIParameterType, error) {
	primary := s.Type.Primary()
	if primary == "" {
		return "", errors.Errorf("OPENAPI_SCHEMA_WITHOUT_TYPE:%s", pointer)
	}
	if s.Format != "" && !openAPIKnownFormats[s.Format] {
		return "", errors.Errorf("OPENAPI_UNSUPPORTED_FORMAT:%q:%s", s.Format, pointer)
	}
	unsupported := func(what string) error {
		return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRAINT:%s:ON_%s:%s", what, primary, pointer)
	}
	switch primary {
	case "string":
		if s.Minimum != nil || s.ExclusiveMinimum != nil {
			return "", unsupported("minimum")
		}
		if s.MinLength != nil {
			if *s.MinLength != 1 || s.Format != "" {
				return "", unsupported(fmt.Sprintf("minLength=%d", *s.MinLength))
			}
			return dxlibTypes.APIParameterTypeNonEmptyString, nil
		}
		switch s.Format {
		case "":
			if s.Type.Nullable() {
				return dxlibTypes.APIParameterTypeNullableString, nil
			}
			return dxlibTypes.APIParameterTypeString, nil
		case "email":
			return dxlibTypes.APIParameterTypeEmail, nil
		case "date-time":
			return dxlibTypes.APIParameterTypeISO8601, nil
		case "date":
			return dxlibTypes.APIParameterTypeDate, nil
		case "time":
			return dxlibTypes.APIParameterTypeTime, nil
		case "binary":
			return dxlibTypes.APIParameterTypeBlob, nil
		case "byte":
			return dxlibTypes.APIParameterTypeEncryptedBlob, nil
		}
		return "", errors.Errorf("OPENAPI_UNSUPPORTED_FORMAT:%q:ON_STRING:%s", s.Format, pointer)
	case "integer":
		if s.MinLength != nil {
			return "", unsupported("minLength")
		}
		if s.ExclusiveMinimum != nil {
			return "", unsupported("exclusiveMinimum")
		}
		wide := s.Format == "" || s.Format == "int64"
		if s.Format != "" && s.Format != "int64" && s.Format != "int32" {
			return "", errors.Errorf("OPENAPI_UNSUPPORTED_FORMAT:%q:ON_INTEGER:%s", s.Format, pointer)
		}
		switch {
		case s.Minimum == nil:
			if s.Type.Nullable() {
				if wide {
					return dxlibTypes.APIParameterTypeNullableInt64, nil
				}
				return dxlibTypes.APIParameterTypeNullableInt32, nil
			}
			if wide {
				return dxlibTypes.APIParameterTypeInt64, nil
			}
			return dxlibTypes.APIParameterTypeInt32, nil
		case *s.Minimum == 0:
			if wide {
				return dxlibTypes.APIParameterTypeInt64ZP, nil
			}
			return dxlibTypes.APIParameterTypeInt32ZP, nil
		case *s.Minimum == 1:
			if wide {
				return dxlibTypes.APIParameterTypeInt64P, nil
			}
			return dxlibTypes.APIParameterTypeInt32P, nil
		}
		return "", unsupported(fmt.Sprintf("minimum=%v", *s.Minimum))
	case "number":
		if s.MinLength != nil {
			return "", unsupported("minLength")
		}
		wide := s.Format == "" || s.Format == "double"
		if s.Format != "" && s.Format != "double" && s.Format != "float" {
			return "", errors.Errorf("OPENAPI_UNSUPPORTED_FORMAT:%q:ON_NUMBER:%s", s.Format, pointer)
		}
		switch {
		case s.Minimum == nil && s.ExclusiveMinimum == nil:
			if wide {
				return dxlibTypes.APIParameterTypeFloat64, nil
			}
			return dxlibTypes.APIParameterTypeFloat32, nil
		case s.Minimum != nil && s.ExclusiveMinimum == nil && *s.Minimum == 0:
			if wide {
				return dxlibTypes.APIParameterTypeFloat64ZP, nil
			}
			return dxlibTypes.APIParameterTypeFloat32ZP, nil
		case s.Minimum == nil && s.ExclusiveMinimum != nil && *s.ExclusiveMinimum == 0:
			if wide {
				return dxlibTypes.APIParameterTypeFloat64P, nil
			}
			return dxlibTypes.APIParameterTypeFloat32P, nil
		}
		return "", unsupported("minimum/exclusiveMinimum")
	case "boolean":
		if s.Format != "" || s.Minimum != nil || s.ExclusiveMinimum != nil || s.MinLength != nil {
			return "", unsupported("format/minimum/minLength")
		}
		return dxlibTypes.APIParameterTypeBoolean, nil
	case "object":
		if s.Format != "" || s.Minimum != nil || s.ExclusiveMinimum != nil || s.MinLength != nil {
			return "", unsupported("format/minimum/minLength")
		}
		if s.AdditionalProperties != nil {
			if s.Properties != nil {
				return "", errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:properties-with-additionalProperties:%s", pointer)
			}
			ap := s.AdditionalProperties
			if ap.Ref != "" || ap.Type.Primary() != "string" || ap.Format != "" || ap.Type.Nullable() {
				return "", errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:additionalProperties-not-string:%s/additionalProperties", pointer)
			}
			return dxlibTypes.APIParameterTypeMapStringString, nil
		}
		if s.Properties != nil {
			return dxlibTypes.APIParameterTypeJSON, nil
		}
		// A free-form object. dxlib's json type builds its value from the
		// declared children and would hand the handler an empty object here;
		// json-passthrough keeps what was sent, which is what "no properties"
		// means.
		return dxlibTypes.APIParameterTypeJSONPassthrough, nil
	case "array":
		if s.Format != "" || s.Minimum != nil || s.ExclusiveMinimum != nil || s.MinLength != nil {
			return "", unsupported("format/minimum/minLength")
		}
		if s.Items == nil {
			return dxlibTypes.APIParameterTypeArray, nil
		}
		items := s.Items
		switch {
		case items.Ref != "":
			// The reference is followed by the caller through the resolver;
			// here an item reference can only mean an object template.
			return dxlibTypes.APIParameterTypeArrayJSONTemplate, nil
		case items.Type.Primary() == "string" && items.Format == "" && !items.Type.Nullable():
			return dxlibTypes.APIParameterTypeArrayString, nil
		case items.Type.Primary() == "integer" && (items.Format == "" || items.Format == "int64") && !items.Type.Nullable() && items.Minimum == nil:
			return dxlibTypes.APIParameterTypeArrayInt64, nil
		case items.Type.Primary() == "object" && items.AdditionalProperties == nil:
			return dxlibTypes.APIParameterTypeArrayJSONTemplate, nil
		}
		return "", errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:array-items-of-%s:%s/items", items.Type.Primary(), pointer)
	}
	return "", errors.Errorf("OPENAPI_UNSUPPORTED_SCHEMA_TYPE:%q:%s", primary, pointer)
}

// openAPIRequestContentTypes maps dxlib's content types to the media type
// keys written under requestBody.content. It is a table of its own rather
// than RequestContentType.String() because that method spells the form
// encoding "application/x-public-form-urlencoded", which no client sends;
// the document carries the registered name.
var openAPIRequestContentTypes = map[utilsHttp.RequestContentType]string{
	utilsHttp.RequestContentTypeApplicationJSON:               "application/json",
	utilsHttp.RequestContentTypeMultiPartFormData:             "multipart/form-data",
	utilsHttp.RequestContentTypeApplicationXWwwFormUrlEncoded: "application/x-www-form-urlencoded",
	utilsHttp.RequestContentTypeApplicationOctetStream:        "application/octet-stream",
	utilsHttp.RequestContentTypeTextPlain:                     "text/plain",
}

func openAPIRequestContentTypeName(t utilsHttp.RequestContentType) (string, error) {
	if t == utilsHttp.RequestContentTypeNone {
		return "", nil
	}
	name, ok := openAPIRequestContentTypes[t]
	if !ok {
		return "", errors.Errorf("OPENAPI_UNMAPPED_REQUEST_CONTENT_TYPE:%d", int(t))
	}
	return name, nil
}

func openAPIRequestContentTypeFromName(name string, pointer string) (utilsHttp.RequestContentType, error) {
	for t, n := range openAPIRequestContentTypes {
		if n == name {
			return t, nil
		}
	}
	return utilsHttp.RequestContentTypeNone, errors.Errorf("OPENAPI_UNSUPPORTED_MEDIA_TYPE:%q:%s", name, pointer)
}

// openAPIEndPointTypes pairs every DXAPIEndPointType with its String() name,
// so x-dxlib-endpoint-type is read back by name and an unknown name is an
// error rather than a zero value.
var openAPIEndPointTypes = []DXAPIEndPointType{
	EndPointTypeHTTPJSON,
	EndPointTypeHTTPUploadStream,
	EndPointTypeHTTPDownloadStream,
	EndPointTypeHTTPDownloadStreamV2,
	EndPointTypeWS,
	EndPointTypeHTTPEndToEndEncryptionV1,
	EndPointTypeHTTPEndToEndEncryptionV2,
	EndPointTypeHTTPEndToEndEncryptionV3,
	EndPointTypeHTTPEndToEndEncryptionV4,
}

func openAPIEndPointTypeFromName(name string, pointer string) (DXAPIEndPointType, error) {
	for _, t := range openAPIEndPointTypes {
		if t.String() == name {
			return t, nil
		}
	}
	return 0, errors.Errorf("OPENAPI_UNKNOWN_ENDPOINT_TYPE:%q:%s", name, pointer)
}
