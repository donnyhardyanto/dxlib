package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/donnyhardyanto/dxlib/errors"
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
)

// The reader: a file -> DXOpenAPIDocument, for the dialect the emitter writes
// and nothing beyond it. Both syntaxes are parsed into one ordered tree --
// JSON through encoding/json's token stream, YAML through yaml.Node -- and a
// single walker reads that tree, so the two formats cannot drift apart. The
// walker is strict on purpose: every object position has a list of the keys
// it understands, a standard key outside the list is an error naming the key
// and its JSON pointer, and the constructs this library deliberately does not
// implement (schema composition, callbacks, webhooks, security requirements,
// remote references) get a message that says so rather than "unknown". Only
// x-* keys outside the x-dxlib-* set are skipped, because that is what the
// extension namespace is for. The reasoning is in OPENAPI.md: a construct
// silently ignored in a definition that gates privileges is the failure this
// design exists to prevent.

// ReadOpenAPIFile reads and validates a document from disk. The syntax is
// chosen by the first non-blank byte: "{" is JSON, anything else is YAML.
func ReadOpenAPIFile(path string) (*DXOpenAPIDocument, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Errorf("OPENAPI_FILE_READ_ERROR:%s:%v", path, err)
	}
	doc, err := ReadOpenAPI(data)
	if err != nil {
		return nil, errors.Wrapf(err, "OPENAPI_FILE:%s", path)
	}
	return doc, nil
}

// ReadOpenAPI parses and validates a document held in memory.
func ReadOpenAPI(data []byte) (*DXOpenAPIDocument, error) {
	trimmed := bytes.TrimLeft(data, " \t\r\n\xef\xbb\xbf")
	if len(trimmed) == 0 {
		return nil, errors.New("OPENAPI_EMPTY_DOCUMENT")
	}
	var root *openAPINode
	var err error
	if trimmed[0] == '{' {
		root, err = openAPIParseJSON(data)
	} else {
		root, err = openAPIParseYAML(data)
	}
	if err != nil {
		return nil, err
	}
	doc, err := openAPIReadDocument(root)
	if err != nil {
		return nil, err
	}
	if err := doc.Validate(); err != nil {
		return nil, err
	}
	return doc, nil
}

// --- the ordered tree -------------------------------------------------------

type openAPINodeKind int

const (
	openAPINodeObject openAPINodeKind = iota
	openAPINodeArray
	openAPINodeString
	openAPINodeNumber
	openAPINodeBool
	openAPINodeNull
)

func (k openAPINodeKind) String() string {
	switch k {
	case openAPINodeObject:
		return "object"
	case openAPINodeArray:
		return "array"
	case openAPINodeString:
		return "string"
	case openAPINodeNumber:
		return "number"
	case openAPINodeBool:
		return "boolean"
	case openAPINodeNull:
		return "null"
	}
	return "?"
}

// openAPINode is one value of either syntax. Objects keep their keys in
// document order; numbers keep their source text, so an int64 at the edge of
// the range is not rounded through a float on the way in.
type openAPINode struct {
	kind   openAPINodeKind
	text   string
	keys   []string
	fields map[string]*openAPINode
	items  []*openAPINode
	line   int
}

func (n *openAPINode) field(key string) *openAPINode {
	if n == nil || n.fields == nil {
		return nil
	}
	return n.fields[key]
}

// openAPIPointerEscape applies RFC 6901's escaping to one reference token.
func openAPIPointerEscape(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "~", "~0"), "/", "~1")
}

func openAPIAt(pointer string, n *openAPINode) string {
	if n != nil && n.line > 0 {
		return fmt.Sprintf("%s:line=%d", pointer, n.line)
	}
	return pointer
}

// --- JSON --------------------------------------------------------------------

type openAPILineIndex []int

func openAPINewLineIndex(data []byte) openAPILineIndex {
	var idx openAPILineIndex
	for i, b := range data {
		if b == '\n' {
			idx = append(idx, i)
		}
	}
	return idx
}

func (idx openAPILineIndex) lineAt(offset int64) int {
	return sort.Search(len(idx), func(i int) bool { return int64(idx[i]) >= offset }) + 1
}

func openAPIParseJSON(data []byte) (*openAPINode, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	lines := openAPINewLineIndex(data)
	root, err := openAPIJSONValue(dec, lines, "")
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.Errorf("OPENAPI_JSON_SYNTAX:TRAILING_CONTENT_AFTER_DOCUMENT:line=%d", lines.lineAt(dec.InputOffset()))
	}
	return root, nil
}

func openAPIJSONValue(dec *json.Decoder, lines openAPILineIndex, pointer string) (*openAPINode, error) {
	tok, err := dec.Token()
	if err != nil {
		if err == io.EOF {
			return nil, errors.Errorf("OPENAPI_JSON_SYNTAX:UNEXPECTED_END:%s", pointer)
		}
		return nil, errors.Errorf("OPENAPI_JSON_SYNTAX:%v:%s:line=%d", err, pointer, lines.lineAt(dec.InputOffset()))
	}
	n := &openAPINode{line: lines.lineAt(dec.InputOffset())}
	switch v := tok.(type) {
	case json.Delim:
		switch v {
		case '{':
			n.kind = openAPINodeObject
			n.fields = map[string]*openAPINode{}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, errors.Errorf("OPENAPI_JSON_SYNTAX:%v:%s:line=%d", err, pointer, lines.lineAt(dec.InputOffset()))
				}
				key, ok := keyTok.(string)
				if !ok {
					return nil, errors.Errorf("OPENAPI_JSON_SYNTAX:OBJECT_KEY_NOT_A_STRING:%s:line=%d", pointer, lines.lineAt(dec.InputOffset()))
				}
				childPointer := pointer + "/" + openAPIPointerEscape(key)
				if _, dup := n.fields[key]; dup {
					return nil, errors.Errorf("OPENAPI_DUPLICATE_KEY:%s:%s:line=%d", key, childPointer, lines.lineAt(dec.InputOffset()))
				}
				child, err := openAPIJSONValue(dec, lines, childPointer)
				if err != nil {
					return nil, err
				}
				n.keys = append(n.keys, key)
				n.fields[key] = child
			}
			if _, err := dec.Token(); err != nil { // the closing brace
				return nil, errors.Errorf("OPENAPI_JSON_SYNTAX:%v:%s", err, pointer)
			}
		case '[':
			n.kind = openAPINodeArray
			for i := 0; dec.More(); i++ {
				child, err := openAPIJSONValue(dec, lines, pointer+"/"+strconv.Itoa(i))
				if err != nil {
					return nil, err
				}
				n.items = append(n.items, child)
			}
			if _, err := dec.Token(); err != nil { // the closing bracket
				return nil, errors.Errorf("OPENAPI_JSON_SYNTAX:%v:%s", err, pointer)
			}
		default:
			return nil, errors.Errorf("OPENAPI_JSON_SYNTAX:UNEXPECTED_%q:%s", string(v), pointer)
		}
	case string:
		n.kind, n.text = openAPINodeString, v
	case json.Number:
		n.kind, n.text = openAPINodeNumber, v.String()
	case bool:
		n.kind = openAPINodeBool
		n.text = strconv.FormatBool(v)
	case nil:
		n.kind = openAPINodeNull
	default:
		return nil, errors.Errorf("OPENAPI_JSON_SYNTAX:UNEXPECTED_TOKEN_%T:%s", tok, pointer)
	}
	return n, nil
}

// --- YAML --------------------------------------------------------------------

func openAPIParseYAML(data []byte) (*openAPINode, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var root yaml.Node
	if err := dec.Decode(&root); err != nil {
		if err == io.EOF {
			return nil, errors.New("OPENAPI_EMPTY_DOCUMENT")
		}
		return nil, errors.Errorf("OPENAPI_YAML_SYNTAX:%v", err)
	}
	// yaml.Unmarshal would read the first document and stop; a second one in
	// the same file is not something to lose without a word.
	var second yaml.Node
	if err := dec.Decode(&second); err != io.EOF {
		if err == nil {
			return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:yaml-multiple-documents:line=%d", second.Line)
		}
		return nil, errors.Errorf("OPENAPI_YAML_SYNTAX:%v", err)
	}
	if root.Kind != yaml.DocumentNode || len(root.Content) != 1 {
		return nil, errors.New("OPENAPI_EMPTY_DOCUMENT")
	}
	return openAPIYAMLValue(root.Content[0], "")
}

func openAPIYAMLValue(y *yaml.Node, pointer string) (*openAPINode, error) {
	n := &openAPINode{line: y.Line}
	switch y.Kind {
	case yaml.MappingNode:
		n.kind = openAPINodeObject
		n.fields = map[string]*openAPINode{}
		for i := 0; i+1 < len(y.Content); i += 2 {
			k, v := y.Content[i], y.Content[i+1]
			if k.Kind != yaml.ScalarNode {
				return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:yaml-non-scalar-key:%s:line=%d", pointer, k.Line)
			}
			if k.ShortTag() == "!!merge" {
				return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:yaml-merge-key:%s:line=%d", pointer, k.Line)
			}
			key := k.Value
			childPointer := pointer + "/" + openAPIPointerEscape(key)
			if _, dup := n.fields[key]; dup {
				return nil, errors.Errorf("OPENAPI_DUPLICATE_KEY:%s:%s:line=%d", key, childPointer, k.Line)
			}
			child, err := openAPIYAMLValue(v, childPointer)
			if err != nil {
				return nil, err
			}
			n.keys = append(n.keys, key)
			n.fields[key] = child
		}
	case yaml.SequenceNode:
		n.kind = openAPINodeArray
		for i, item := range y.Content {
			child, err := openAPIYAMLValue(item, pointer+"/"+strconv.Itoa(i))
			if err != nil {
				return nil, err
			}
			n.items = append(n.items, child)
		}
	case yaml.ScalarNode:
		switch y.ShortTag() {
		case "!!str":
			n.kind, n.text = openAPINodeString, y.Value
		case "!!int", "!!float":
			n.kind, n.text = openAPINodeNumber, y.Value
		case "!!bool":
			n.kind = openAPINodeBool
			b, err := strconv.ParseBool(strings.ToLower(y.Value))
			if err != nil {
				return nil, errors.Errorf("OPENAPI_YAML_SYNTAX:BAD_BOOLEAN:%q:%s:line=%d", y.Value, pointer, y.Line)
			}
			n.text = strconv.FormatBool(b)
		case "!!null":
			n.kind = openAPINodeNull
		default:
			// !!timestamp for an unquoted date, !!binary, a custom !tag: none
			// has a place in this dialect. Quoting the value makes it a string.
			return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:yaml-tag-%s:%s:line=%d:QUOTE_THE_VALUE_FOR_A_STRING", y.ShortTag(), pointer, y.Line)
		}
	case yaml.AliasNode:
		return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:yaml-alias:%s:line=%d", pointer, y.Line)
	default:
		return nil, errors.Errorf("OPENAPI_YAML_SYNTAX:UNEXPECTED_NODE_KIND_%d:%s:line=%d", y.Kind, pointer, y.Line)
	}
	return n, nil
}

// --- the walker --------------------------------------------------------------

// openAPIRefusedFields are standard OpenAPI and JSON Schema keys this reader
// recognises and does not implement. Each gets a message that names it and
// says why, so the author of a document is not told a well-known keyword is
// "unknown". A key absent from every list is unknown and is an error too.
var openAPIRefusedFields = map[string]string{
	"oneOf": "SCHEMA_COMPOSITION_NOT_SUPPORTED", "anyOf": "SCHEMA_COMPOSITION_NOT_SUPPORTED",
	"allOf": "SCHEMA_COMPOSITION_NOT_SUPPORTED", "not": "SCHEMA_COMPOSITION_NOT_SUPPORTED",
	"discriminator":   "SCHEMA_COMPOSITION_NOT_SUPPORTED",
	"callbacks":       "CALLBACKS_NOT_SUPPORTED",
	"webhooks":        "WEBHOOKS_NOT_SUPPORTED",
	"security":        "SECURITY_REQUIREMENTS_ARE_NOT_ENFORCED_BY_DXLIB_AND_ARE_REFUSED_RATHER_THAN_IGNORED",
	"securitySchemes": "SECURITY_REQUIREMENTS_ARE_NOT_ENFORCED_BY_DXLIB_AND_ARE_REFUSED_RATHER_THAN_IGNORED",
	"servers":         "NOT_CARRIED_BY_DXLIB", "tags": "NOT_CARRIED_BY_DXLIB", "externalDocs": "NOT_CARRIED_BY_DXLIB",
	"deprecated": "NOT_CARRIED_BY_DXLIB", "links": "NOT_CARRIED_BY_DXLIB", "examples": "NOT_CARRIED_BY_DXLIB",
	"example": "NOT_CARRIED_BY_DXLIB", "encoding": "NOT_CARRIED_BY_DXLIB", "style": "NOT_CARRIED_BY_DXLIB",
	"explode": "NOT_CARRIED_BY_DXLIB", "allowReserved": "NOT_CARRIED_BY_DXLIB", "allowEmptyValue": "NOT_CARRIED_BY_DXLIB",
	"jsonSchemaDialect": "NOT_CARRIED_BY_DXLIB", "contact": "NOT_CARRIED_BY_DXLIB", "license": "NOT_CARRIED_BY_DXLIB",
	"termsOfService": "NOT_CARRIED_BY_DXLIB",
	"$schema":        "NOT_CARRIED_BY_DXLIB", "$id": "NOT_CARRIED_BY_DXLIB", "$defs": "NOT_CARRIED_BY_DXLIB",
	"$dynamicRef": "NOT_CARRIED_BY_DXLIB", "$anchor": "NOT_CARRIED_BY_DXLIB", "$comment": "NOT_CARRIED_BY_DXLIB",
	"const": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB", "pattern": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB",
	"maximum": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB", "exclusiveMaximum": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB",
	"maxLength": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB", "minItems": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB",
	"maxItems": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB", "uniqueItems": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB",
	"multipleOf": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB", "minProperties": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB",
	"maxProperties": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB", "patternProperties": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB",
	"prefixItems": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB", "contains": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB",
	"dependentRequired": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB", "if": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB",
	"then": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB", "else": "CONSTRAINT_NOT_ENFORCED_BY_DXLIB",
	"default": "NOT_CARRIED_BY_DXLIB", "readOnly": "NOT_CARRIED_BY_DXLIB", "writeOnly": "NOT_CARRIED_BY_DXLIB",
	"title": "NOT_CARRIED_BY_DXLIB", "contentMediaType": "NOT_CARRIED_BY_DXLIB", "contentEncoding": "NOT_CARRIED_BY_DXLIB",
	"nullable": "OPENAPI_3.0_KEYWORD:USE_type_[T,null]",
}

// openAPIFields checks an object against the keys a position understands.
// The x-* namespace is skipped unless a key is one of ours -- those are in
// allowed and read by the caller.
func openAPIFields(n *openAPINode, pointer string, allowed ...string) error {
	if n.kind != openAPINodeObject {
		return errors.Errorf("OPENAPI_WRONG_TYPE:%s:EXPECTED_object:GOT_%s", openAPIAt(pointer, n), n.kind)
	}
	ok := map[string]bool{}
	for _, a := range allowed {
		ok[a] = true
	}
	for _, key := range n.keys {
		if ok[key] {
			continue
		}
		childPointer := pointer + "/" + openAPIPointerEscape(key)
		if reason, refused := openAPIRefusedFields[key]; refused && reason != "" {
			return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:%s:%s:%s", key, openAPIAt(childPointer, n.fields[key]), reason)
		}
		if strings.HasPrefix(key, "x-") {
			if strings.HasPrefix(key, "x-dxlib-") {
				return errors.Errorf("OPENAPI_UNKNOWN_DXLIB_EXTENSION:%s:%s", key, openAPIAt(childPointer, n.fields[key]))
			}
			continue
		}
		return errors.Errorf("OPENAPI_UNKNOWN_FIELD:%s:%s", key, openAPIAt(childPointer, n.fields[key]))
	}
	return nil
}

func openAPIWrongType(n *openAPINode, pointer, expected string) error {
	return errors.Errorf("OPENAPI_WRONG_TYPE:%s:EXPECTED_%s:GOT_%s", openAPIAt(pointer, n), expected, n.kind)
}

func openAPIMissing(pointer, field string) error {
	return errors.Errorf("OPENAPI_REQUIRED_FIELD_MISSING:%s:%s", field, pointer)
}

// openAPIString reads an optional string field; the bool says whether it was
// present.
func openAPIString(parent *openAPINode, pointer, field string) (string, bool, error) {
	n := parent.field(field)
	if n == nil {
		return "", false, nil
	}
	if n.kind != openAPINodeString {
		return "", true, openAPIWrongType(n, pointer+"/"+field, "string")
	}
	return n.text, true, nil
}

func openAPIRequiredString(parent *openAPINode, pointer, field string) (string, error) {
	s, present, err := openAPIString(parent, pointer, field)
	if err != nil {
		return "", err
	}
	if !present {
		return "", openAPIMissing(pointer, field)
	}
	return s, nil
}

func openAPIBool(parent *openAPINode, pointer, field string) (bool, error) {
	n := parent.field(field)
	if n == nil {
		return false, nil
	}
	if n.kind != openAPINodeBool {
		return false, openAPIWrongType(n, pointer+"/"+field, "boolean")
	}
	return n.text == "true", nil
}

func openAPIFloat64(parent *openAPINode, pointer, field string) (*float64, error) {
	n := parent.field(field)
	if n == nil {
		return nil, nil
	}
	if n.kind != openAPINodeNumber {
		return nil, openAPIWrongType(n, pointer+"/"+field, "number")
	}
	f, err := strconv.ParseFloat(n.text, 64)
	if err != nil {
		return nil, errors.Errorf("OPENAPI_BAD_NUMBER:%q:%s", n.text, openAPIAt(pointer+"/"+field, n))
	}
	return &f, nil
}

func openAPIInt64(parent *openAPINode, pointer, field string) (*int64, error) {
	n := parent.field(field)
	if n == nil {
		return nil, nil
	}
	if n.kind != openAPINodeNumber {
		return nil, openAPIWrongType(n, pointer+"/"+field, "integer")
	}
	i, err := strconv.ParseInt(n.text, 10, 64)
	if err != nil {
		return nil, errors.Errorf("OPENAPI_BAD_INTEGER:%q:%s", n.text, openAPIAt(pointer+"/"+field, n))
	}
	return &i, nil
}

func openAPIStringList(parent *openAPINode, pointer, field string) ([]string, error) {
	n := parent.field(field)
	if n == nil {
		return nil, nil
	}
	if n.kind != openAPINodeArray {
		return nil, openAPIWrongType(n, pointer+"/"+field, "array")
	}
	out := make([]string, 0, len(n.items))
	for i, item := range n.items {
		if item.kind != openAPINodeString {
			return nil, openAPIWrongType(item, fmt.Sprintf("%s/%s/%d", pointer, field, i), "string")
		}
		out = append(out, item.text)
	}
	return out, nil
}

// openAPIScalar turns an enum member into the Go value dxlib compares
// against. Numbers become int64 when they have no fraction, so an enum of
// codes stays integral; Validate compares by formatted text anyway.
func openAPIScalar(n *openAPINode, pointer string) (any, error) {
	switch n.kind {
	case openAPINodeString:
		return n.text, nil
	case openAPINodeBool:
		return n.text == "true", nil
	case openAPINodeNull:
		return nil, nil
	case openAPINodeNumber:
		if i, err := strconv.ParseInt(n.text, 10, 64); err == nil {
			return i, nil
		}
		f, err := strconv.ParseFloat(n.text, 64)
		if err != nil {
			return nil, errors.Errorf("OPENAPI_BAD_NUMBER:%q:%s", n.text, openAPIAt(pointer, n))
		}
		return f, nil
	}
	return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:non-scalar-enum-member:%s", openAPIAt(pointer, n))
}

// --- document ------------------------------------------------------------------

func openAPIReadDocument(root *openAPINode) (*DXOpenAPIDocument, error) {
	if err := openAPIFields(root, "", "openapi", "info", "paths", "components", OpenAPIExtensionWebSocketEndPoints); err != nil {
		return nil, err
	}
	doc := &DXOpenAPIDocument{Paths: NewDXOpenAPIOrderedMap[*DXOpenAPIPathItem]()}
	var err error
	if doc.OpenAPI, err = openAPIRequiredString(root, "", "openapi"); err != nil {
		return nil, err
	}

	info := root.field("info")
	if info == nil {
		return nil, openAPIMissing("", "info")
	}
	if err := openAPIFields(info, "/info", "title", "version", "description"); err != nil {
		return nil, err
	}
	if doc.Info.Title, err = openAPIRequiredString(info, "/info", "title"); err != nil {
		return nil, err
	}
	if doc.Info.Version, err = openAPIRequiredString(info, "/info", "version"); err != nil {
		return nil, err
	}
	if doc.Info.Description, _, err = openAPIString(info, "/info", "description"); err != nil {
		return nil, err
	}

	paths := root.field("paths")
	if paths == nil {
		return nil, openAPIMissing("", "paths")
	}
	if paths.kind != openAPINodeObject {
		return nil, openAPIWrongType(paths, "/paths", "object")
	}
	for _, path := range paths.keys {
		pointer := "/paths/" + openAPIPointerEscape(path)
		item, err := openAPIReadPathItem(paths.fields[path], pointer)
		if err != nil {
			return nil, err
		}
		doc.Paths.Set(path, item)
	}

	if components := root.field("components"); components != nil {
		if err := openAPIFields(components, "/components", "schemas"); err != nil {
			return nil, err
		}
		doc.Components = &DXOpenAPIComponents{}
		if schemas := components.field("schemas"); schemas != nil {
			if schemas.kind != openAPINodeObject {
				return nil, openAPIWrongType(schemas, "/components/schemas", "object")
			}
			doc.Components.Schemas = NewDXOpenAPIOrderedMap[*DXOpenAPISchema]()
			for _, name := range schemas.keys {
				s, err := openAPIReadSchema(schemas.fields[name], "/components/schemas/"+openAPIPointerEscape(name))
				if err != nil {
					return nil, err
				}
				doc.Components.Schemas.Set(name, s)
			}
		}
	}

	if ws := root.field(OpenAPIExtensionWebSocketEndPoints); ws != nil {
		pointer := "/" + OpenAPIExtensionWebSocketEndPoints
		if err := openAPIFields(ws, pointer, "description", "endpoints"); err != nil {
			return nil, err
		}
		doc.WebSocketEndPoints = &DXOpenAPIWebSocketEndPoints{}
		if doc.WebSocketEndPoints.Description, err = openAPIRequiredString(ws, pointer, "description"); err != nil {
			return nil, err
		}
		list := ws.field("endpoints")
		if list == nil {
			return nil, openAPIMissing(pointer, "endpoints")
		}
		if list.kind != openAPINodeArray {
			return nil, openAPIWrongType(list, pointer+"/endpoints", "array")
		}
		for i, entry := range list.items {
			e, err := openAPIReadWebSocketEndPoint(entry, fmt.Sprintf("%s/endpoints/%d", pointer, i))
			if err != nil {
				return nil, err
			}
			doc.WebSocketEndPoints.EndPoints = append(doc.WebSocketEndPoints.EndPoints, e)
		}
	}
	return doc, nil
}

func openAPIReadPathItem(n *openAPINode, pointer string) (*DXOpenAPIPathItem, error) {
	if n.kind == openAPINodeObject {
		for _, key := range n.keys {
			switch key {
			case "parameters":
				return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:path-level-parameters:%s:DECLARE_THEM_ON_EACH_OPERATION", openAPIAt(pointer+"/parameters", n.fields[key]))
			case "$ref":
				return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:path-item-$ref:%s", openAPIAt(pointer+"/$ref", n.fields[key]))
			}
		}
	}
	allowed := []string{"summary", "description"}
	for _, m := range openAPIMethods {
		allowed = append(allowed, m.key)
	}
	if err := openAPIFields(n, pointer, allowed...); err != nil {
		return nil, err
	}
	item := &DXOpenAPIPathItem{}
	var err error
	if item.Summary, _, err = openAPIString(n, pointer, "summary"); err != nil {
		return nil, err
	}
	if item.Description, _, err = openAPIString(n, pointer, "description"); err != nil {
		return nil, err
	}
	for _, m := range openAPIMethods {
		opNode := n.field(m.key)
		if opNode == nil {
			continue
		}
		op, err := openAPIReadOperation(opNode, pointer+"/"+m.key)
		if err != nil {
			return nil, err
		}
		*item.operation(m.method) = op
	}
	return item, nil
}

func openAPIReadOperation(n *openAPINode, pointer string) (*DXOpenAPIOperation, error) {
	if err := openAPIFields(n, pointer, "operationId", "summary", "description", "parameters", "requestBody", "responses",
		OpenAPIExtensionEndPointType, OpenAPIExtensionPrivileges, OpenAPIExtensionRateLimitGroup,
		OpenAPIExtensionMaxContentLength, OpenAPIExtensionRequestContentType, OpenAPIExtensionParameters); err != nil {
		return nil, err
	}
	op := &DXOpenAPIOperation{}
	var err error
	if op.OperationId, err = openAPIRequiredString(n, pointer, "operationId"); err != nil {
		return nil, err
	}
	if op.Summary, _, err = openAPIString(n, pointer, "summary"); err != nil {
		return nil, err
	}
	if op.Description, _, err = openAPIString(n, pointer, "description"); err != nil {
		return nil, err
	}
	if params := n.field("parameters"); params != nil {
		if params.kind != openAPINodeArray {
			return nil, openAPIWrongType(params, pointer+"/parameters", "array")
		}
		for i, pn := range params.items {
			p, err := openAPIReadParameter(pn, fmt.Sprintf("%s/parameters/%d", pointer, i))
			if err != nil {
				return nil, err
			}
			op.Parameters = append(op.Parameters, p)
		}
	}
	if rb := n.field("requestBody"); rb != nil {
		if op.RequestBody, err = openAPIReadRequestBody(rb, pointer+"/requestBody"); err != nil {
			return nil, err
		}
	}
	if rs := n.field("responses"); rs != nil {
		if op.Responses, err = openAPIReadResponses(rs, pointer+"/responses"); err != nil {
			return nil, err
		}
	}
	if op.EndPointType, _, err = openAPIString(n, pointer, OpenAPIExtensionEndPointType); err != nil {
		return nil, err
	}
	if op.Privileges, err = openAPIStringList(n, pointer, OpenAPIExtensionPrivileges); err != nil {
		return nil, err
	}
	if op.RateLimitGroup, _, err = openAPIString(n, pointer, OpenAPIExtensionRateLimitGroup); err != nil {
		return nil, err
	}
	max, err := openAPIInt64(n, pointer, OpenAPIExtensionMaxContentLength)
	if err != nil {
		return nil, err
	}
	if max != nil {
		if *max < 0 {
			return nil, errors.Errorf("OPENAPI_NEGATIVE_MAX_CONTENT_LENGTH:%d:%s/%s", *max, pointer, OpenAPIExtensionMaxContentLength)
		}
		op.MaxContentLength = *max
	}
	if op.RequestContentType, _, err = openAPIString(n, pointer, OpenAPIExtensionRequestContentType); err != nil {
		return nil, err
	}
	if ps := n.field(OpenAPIExtensionParameters); ps != nil {
		if op.ParametersSchema, err = openAPIReadSchema(ps, pointer+"/"+OpenAPIExtensionParameters); err != nil {
			return nil, err
		}
	}
	return op, nil
}

func openAPIReadParameter(n *openAPINode, pointer string) (*DXOpenAPIParameter, error) {
	if n.kind == openAPINodeObject && n.field("$ref") != nil {
		return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:parameter-$ref:%s:ONLY_SCHEMA_$ref_INTO_components/schemas_IS_SUPPORTED", openAPIAt(pointer+"/$ref", n.field("$ref")))
	}
	if n.kind == openAPINodeObject && n.field("content") != nil {
		return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:parameter-content:%s:USE_schema", openAPIAt(pointer+"/content", n.field("content")))
	}
	if err := openAPIFields(n, pointer, "name", "in", "description", "required", "schema"); err != nil {
		return nil, err
	}
	p := &DXOpenAPIParameter{}
	var err error
	if p.Name, err = openAPIRequiredString(n, pointer, "name"); err != nil {
		return nil, err
	}
	if p.In, err = openAPIRequiredString(n, pointer, "in"); err != nil {
		return nil, err
	}
	if p.Description, _, err = openAPIString(n, pointer, "description"); err != nil {
		return nil, err
	}
	if p.Required, err = openAPIBool(n, pointer, "required"); err != nil {
		return nil, err
	}
	schema := n.field("schema")
	if schema == nil {
		return nil, openAPIMissing(pointer, "schema")
	}
	if p.Schema, err = openAPIReadSchema(schema, pointer+"/schema"); err != nil {
		return nil, err
	}
	return p, nil
}

func openAPIReadRequestBody(n *openAPINode, pointer string) (*DXOpenAPIRequestBody, error) {
	if n.kind == openAPINodeObject && n.field("$ref") != nil {
		return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:requestBody-$ref:%s:ONLY_SCHEMA_$ref_INTO_components/schemas_IS_SUPPORTED", openAPIAt(pointer+"/$ref", n.field("$ref")))
	}
	if err := openAPIFields(n, pointer, "description", "required", "content"); err != nil {
		return nil, err
	}
	rb := &DXOpenAPIRequestBody{}
	var err error
	if rb.Description, _, err = openAPIString(n, pointer, "description"); err != nil {
		return nil, err
	}
	if rb.Required, err = openAPIBool(n, pointer, "required"); err != nil {
		return nil, err
	}
	content := n.field("content")
	if content == nil {
		return nil, openAPIMissing(pointer, "content")
	}
	if rb.Content, err = openAPIReadContent(content, pointer+"/content"); err != nil {
		return nil, err
	}
	return rb, nil
}

func openAPIReadContent(n *openAPINode, pointer string) (*DXOpenAPIOrderedMap[*DXOpenAPIMediaType], error) {
	if n.kind != openAPINodeObject {
		return nil, openAPIWrongType(n, pointer, "object")
	}
	content := NewDXOpenAPIOrderedMap[*DXOpenAPIMediaType]()
	for _, mediaType := range n.keys {
		mtPointer := pointer + "/" + openAPIPointerEscape(mediaType)
		mtNode := n.fields[mediaType]
		if err := openAPIFields(mtNode, mtPointer, "schema"); err != nil {
			return nil, err
		}
		mt := &DXOpenAPIMediaType{}
		if s := mtNode.field("schema"); s != nil {
			var err error
			if mt.Schema, err = openAPIReadSchema(s, mtPointer+"/schema"); err != nil {
				return nil, err
			}
		}
		content.Set(mediaType, mt)
	}
	return content, nil
}

func openAPIReadResponses(n *openAPINode, pointer string) (*DXOpenAPIOrderedMap[*DXOpenAPIResponse], error) {
	if n.kind != openAPINodeObject {
		return nil, openAPIWrongType(n, pointer, "object")
	}
	responses := NewDXOpenAPIOrderedMap[*DXOpenAPIResponse]()
	for _, code := range n.keys {
		rPointer := pointer + "/" + openAPIPointerEscape(code)
		rn := n.fields[code]
		if rn.kind == openAPINodeObject && rn.field("$ref") != nil {
			return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:response-$ref:%s", openAPIAt(rPointer+"/$ref", rn.field("$ref")))
		}
		if err := openAPIFields(rn, rPointer, "description", "headers", "content", OpenAPIExtensionResponseName); err != nil {
			return nil, err
		}
		r := &DXOpenAPIResponse{}
		var err error
		if r.Description, err = openAPIRequiredString(rn, rPointer, "description"); err != nil {
			return nil, err
		}
		if r.Name, _, err = openAPIString(rn, rPointer, OpenAPIExtensionResponseName); err != nil {
			return nil, err
		}
		if headers := rn.field("headers"); headers != nil {
			if headers.kind != openAPINodeObject {
				return nil, openAPIWrongType(headers, rPointer+"/headers", "object")
			}
			r.Headers = NewDXOpenAPIOrderedMap[*DXOpenAPIHeader]()
			for _, name := range headers.keys {
				hPointer := rPointer + "/headers/" + openAPIPointerEscape(name)
				hn := headers.fields[name]
				if hn.kind == openAPINodeObject && hn.field("$ref") != nil {
					return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:header-$ref:%s", openAPIAt(hPointer+"/$ref", hn.field("$ref")))
				}
				if err := openAPIFields(hn, hPointer, "description", "required", "schema"); err != nil {
					return nil, err
				}
				h := &DXOpenAPIHeader{}
				if h.Description, _, err = openAPIString(hn, hPointer, "description"); err != nil {
					return nil, err
				}
				if h.Required, err = openAPIBool(hn, hPointer, "required"); err != nil {
					return nil, err
				}
				if s := hn.field("schema"); s != nil {
					if h.Schema, err = openAPIReadSchema(s, hPointer+"/schema"); err != nil {
						return nil, err
					}
				}
				r.Headers.Set(name, h)
			}
		}
		if content := rn.field("content"); content != nil {
			if r.Content, err = openAPIReadContent(content, rPointer+"/content"); err != nil {
				return nil, err
			}
		}
		responses.Set(code, r)
	}
	return responses, nil
}

func openAPIReadSchema(n *openAPINode, pointer string) (*DXOpenAPISchema, error) {
	if n.kind == openAPINodeBool {
		return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:boolean-schema:%s", openAPIAt(pointer, n))
	}
	if err := openAPIFields(n, pointer, "$ref", "type", "format", "description", "properties", "required", "items",
		"additionalProperties", "enum", "minimum", "exclusiveMinimum", "minLength", OpenAPIExtensionType); err != nil {
		return nil, err
	}
	s := &DXOpenAPISchema{}
	var err error
	if s.Ref, _, err = openAPIString(n, pointer, "$ref"); err != nil {
		return nil, err
	}
	if s.Ref != "" {
		// A reference stands alone. Siblings would have to be merged with the
		// target, which is composition by another name.
		for _, key := range n.keys {
			if key != "$ref" && !strings.HasPrefix(key, "x-") {
				return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:$ref-with-sibling-%s:%s", key, openAPIAt(pointer, n))
			}
		}
		if _, err := openAPIRefName(s.Ref); err != nil {
			return nil, errors.Wrapf(err, "OPENAPI_AT:%s", openAPIAt(pointer+"/$ref", n.field("$ref")))
		}
		return s, nil
	}
	if t := n.field("type"); t != nil {
		switch t.kind {
		case openAPINodeString:
			s.Type = DXOpenAPISchemaType{t.text}
		case openAPINodeArray:
			for i, item := range t.items {
				if item.kind != openAPINodeString {
					return nil, openAPIWrongType(item, fmt.Sprintf("%s/type/%d", pointer, i), "string")
				}
				s.Type = append(s.Type, item.text)
			}
		default:
			return nil, openAPIWrongType(t, pointer+"/type", "string_or_array")
		}
	}
	if s.Format, _, err = openAPIString(n, pointer, "format"); err != nil {
		return nil, err
	}
	if s.Description, _, err = openAPIString(n, pointer, "description"); err != nil {
		return nil, err
	}
	if props := n.field("properties"); props != nil {
		if props.kind != openAPINodeObject {
			return nil, openAPIWrongType(props, pointer+"/properties", "object")
		}
		s.Properties = NewDXOpenAPIOrderedMap[*DXOpenAPISchema]()
		for _, name := range props.keys {
			child, err := openAPIReadSchema(props.fields[name], pointer+"/properties/"+openAPIPointerEscape(name))
			if err != nil {
				return nil, err
			}
			s.Properties.Set(name, child)
		}
	}
	if s.Required, err = openAPIStringList(n, pointer, "required"); err != nil {
		return nil, err
	}
	if items := n.field("items"); items != nil {
		if s.Items, err = openAPIReadSchema(items, pointer+"/items"); err != nil {
			return nil, err
		}
	}
	if ap := n.field("additionalProperties"); ap != nil {
		if ap.kind == openAPINodeBool {
			return nil, errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:additionalProperties-boolean:%s:USE_A_SCHEMA_OR_OMIT", openAPIAt(pointer+"/additionalProperties", ap))
		}
		if s.AdditionalProperties, err = openAPIReadSchema(ap, pointer+"/additionalProperties"); err != nil {
			return nil, err
		}
	}
	if enum := n.field("enum"); enum != nil {
		if enum.kind != openAPINodeArray {
			return nil, openAPIWrongType(enum, pointer+"/enum", "array")
		}
		if len(enum.items) == 0 {
			return nil, errors.Errorf("OPENAPI_EMPTY_ENUM:%s", openAPIAt(pointer+"/enum", enum))
		}
		for i, item := range enum.items {
			v, err := openAPIScalar(item, fmt.Sprintf("%s/enum/%d", pointer, i))
			if err != nil {
				return nil, err
			}
			s.Enum = append(s.Enum, v)
		}
	}
	if s.Minimum, err = openAPIFloat64(n, pointer, "minimum"); err != nil {
		return nil, err
	}
	if s.ExclusiveMinimum, err = openAPIFloat64(n, pointer, "exclusiveMinimum"); err != nil {
		return nil, err
	}
	minLength, err := openAPIInt64(n, pointer, "minLength")
	if err != nil {
		return nil, err
	}
	if minLength != nil {
		if *minLength < 0 {
			return nil, errors.Errorf("OPENAPI_NEGATIVE_MIN_LENGTH:%d:%s", *minLength, openAPIAt(pointer+"/minLength", n.field("minLength")))
		}
		v := int(*minLength)
		s.MinLength = &v
	}
	if s.DXLibType, _, err = openAPIString(n, pointer, OpenAPIExtensionType); err != nil {
		return nil, err
	}
	return s, nil
}

func openAPIReadWebSocketEndPoint(n *openAPINode, pointer string) (*DXOpenAPIWebSocketEndPoint, error) {
	if err := openAPIFields(n, pointer, "operationId", "path", "method", "summary", "description", "privileges", "rateLimitGroup", "periodicInterval"); err != nil {
		return nil, err
	}
	e := &DXOpenAPIWebSocketEndPoint{}
	var err error
	if e.OperationId, err = openAPIRequiredString(n, pointer, "operationId"); err != nil {
		return nil, err
	}
	if e.Path, err = openAPIRequiredString(n, pointer, "path"); err != nil {
		return nil, err
	}
	if e.Method, err = openAPIRequiredString(n, pointer, "method"); err != nil {
		return nil, err
	}
	if e.Summary, _, err = openAPIString(n, pointer, "summary"); err != nil {
		return nil, err
	}
	if e.Description, _, err = openAPIString(n, pointer, "description"); err != nil {
		return nil, err
	}
	if e.Privileges, err = openAPIStringList(n, pointer, "privileges"); err != nil {
		return nil, err
	}
	if e.RateLimitGroup, _, err = openAPIString(n, pointer, "rateLimitGroup"); err != nil {
		return nil, err
	}
	if e.PeriodicInterval, _, err = openAPIString(n, pointer, "periodicInterval"); err != nil {
		return nil, err
	}
	return e, nil
}

// --- validation ------------------------------------------------------------------

// Validate checks what the walker cannot see one node at a time: the version,
// that every operationId is unique across paths and the WebSocket list, that
// path templates and path parameters agree, that references resolve, and
// that every value drawn from a fixed vocabulary is in it. ReadOpenAPI runs
// it, and BindOpenAPI runs it again so a document built in code is held to
// the same rules as one read from a file.
func (doc *DXOpenAPIDocument) Validate() error {
	if !strings.HasPrefix(doc.OpenAPI, "3.1.") {
		return errors.Errorf("OPENAPI_VERSION_UNSUPPORTED:%q:/openapi:ONLY_3.1.x_IS_SUPPORTED", doc.OpenAPI)
	}
	if doc.Info.Title == "" {
		return openAPIMissing("/info", "title")
	}
	if doc.Info.Version == "" {
		return openAPIMissing("/info", "version")
	}
	if doc.Paths == nil {
		return openAPIMissing("", "paths")
	}

	v := &openAPIValidator{doc: doc, operationIds: map[string]string{}}
	if doc.Components != nil && doc.Components.Schemas != nil {
		for _, name := range doc.Components.Schemas.Keys() {
			s, _ := doc.Components.Schemas.Get(name)
			if err := v.schema(s, "/components/schemas/"+openAPIPointerEscape(name)); err != nil {
				return err
			}
		}
	}
	for _, path := range doc.Paths.Keys() {
		item, _ := doc.Paths.Get(path)
		pointer := "/paths/" + openAPIPointerEscape(path)
		if !strings.HasPrefix(path, "/") {
			return errors.Errorf("OPENAPI_PATH_MUST_START_WITH_SLASH:%q:%s", path, pointer)
		}
		templateNames, err := openAPIPathTemplateNames(path)
		if err != nil {
			return errors.Wrapf(err, "OPENAPI_AT:%s", pointer)
		}
		methods, ops := item.Operations()
		if len(ops) == 0 {
			return errors.Errorf("OPENAPI_PATH_WITHOUT_OPERATIONS:%s", pointer)
		}
		// dxlib registers one endpoint per URI and checks the method inside
		// it, so a path item with two methods has no endpoint to become.
		if len(ops) > 1 {
			return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:multiple-methods-on-one-path:%s:DXLIB_REGISTERS_ONE_METHOD_PER_URI", pointer)
		}
		for i, op := range ops {
			if err := v.operation(op, templateNames, methods[i], pointer+"/"+strings.ToLower(methods[i])); err != nil {
				return err
			}
		}
	}
	if doc.WebSocketEndPoints != nil {
		for i, ws := range doc.WebSocketEndPoints.EndPoints {
			pointer := fmt.Sprintf("/%s/endpoints/%d", OpenAPIExtensionWebSocketEndPoints, i)
			if err := v.claimOperationId(ws.OperationId, pointer); err != nil {
				return err
			}
			if !strings.HasPrefix(ws.Path, "/") {
				return errors.Errorf("OPENAPI_PATH_MUST_START_WITH_SLASH:%q:%s/path", ws.Path, pointer)
			}
			if _, err := openAPIPathTemplateNames(ws.Path); err != nil {
				return errors.Wrapf(err, "OPENAPI_AT:%s/path", pointer)
			}
			if ws.Method == "" || ws.Method != strings.ToUpper(ws.Method) {
				return errors.Errorf("OPENAPI_WS_METHOD_MUST_BE_UPPER_CASE:%q:%s/method", ws.Method, pointer)
			}
			if ws.PeriodicInterval != "" {
				d, err := time.ParseDuration(ws.PeriodicInterval)
				if err != nil || d <= 0 {
					return errors.Errorf("OPENAPI_WS_BAD_PERIODIC_INTERVAL:%q:%s/periodicInterval:EXPECTED_A_POSITIVE_GO_DURATION", ws.PeriodicInterval, pointer)
				}
			}
		}
	}
	return nil
}

type openAPIValidator struct {
	doc          *DXOpenAPIDocument
	operationIds map[string]string
}

func (v *openAPIValidator) claimOperationId(id, pointer string) error {
	if id == "" {
		return openAPIMissing(pointer, "operationId")
	}
	if first, taken := v.operationIds[id]; taken {
		return errors.Errorf("OPENAPI_DUPLICATE_OPERATION_ID:%s:%s:%s", id, first, pointer)
	}
	v.operationIds[id] = pointer
	return nil
}

func (v *openAPIValidator) operation(op *DXOpenAPIOperation, templateNames []string, method, pointer string) error {
	if err := v.claimOperationId(op.OperationId, pointer); err != nil {
		return err
	}
	if op.EndPointType != "" {
		t, err := openAPIEndPointTypeFromName(op.EndPointType, pointer+"/"+OpenAPIExtensionEndPointType)
		if err != nil {
			return err
		}
		if t == EndPointTypeWS {
			return errors.Errorf("OPENAPI_WS_ENDPOINT_UNDER_PATHS:%s:USE_%s", pointer, OpenAPIExtensionWebSocketEndPoints)
		}
	}
	names := map[string]bool{}
	pathParams := map[string]bool{}
	for i, p := range op.Parameters {
		pPointer := fmt.Sprintf("%s/parameters/%d", pointer, i)
		if names[p.Name] {
			return errors.Errorf("OPENAPI_DUPLICATE_PARAMETER:%s:%s", p.Name, pPointer)
		}
		names[p.Name] = true
		switch p.In {
		case "query":
			// PreProcessRequest reads the query string for GET and DELETE and
			// the body for POST and PUT; a query parameter on a body method
			// would be declared and never read.
			if openAPIBodyMethods[method] {
				return errors.Errorf("OPENAPI_QUERY_PARAMETER_ON_%s:%s:%s:DXLIB_READS_THE_BODY_FOR_POST_AND_PUT", method, p.Name, pPointer)
			}
		case "path":
			if !p.Required {
				return errors.Errorf("OPENAPI_PATH_PARAMETER_MUST_BE_REQUIRED:%s:%s", p.Name, pPointer)
			}
			pathParams[p.Name] = true
		case "header", "cookie":
			return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:parameter-in-%s:%s/in:ONLY_query_AND_path_ARE_BOUND", p.In, pPointer)
		default:
			return errors.Errorf("OPENAPI_BAD_PARAMETER_LOCATION:%q:%s/in", p.In, pPointer)
		}
		if p.Schema == nil {
			return openAPIMissing(pPointer, "schema")
		}
		if err := v.schema(p.Schema, pPointer+"/schema"); err != nil {
			return err
		}
	}
	for _, name := range templateNames {
		if !pathParams[name] {
			return errors.Errorf("OPENAPI_PATH_TEMPLATE_WITHOUT_PARAMETER:{%s}:%s:DECLARE_in=path", name, pointer)
		}
	}
	for name := range pathParams {
		found := false
		for _, t := range templateNames {
			if t == name {
				found = true
			}
		}
		if !found {
			return errors.Errorf("OPENAPI_PATH_PARAMETER_NOT_IN_TEMPLATE:%s:%s", name, pointer)
		}
	}
	if op.RequestBody != nil {
		rbPointer := pointer + "/requestBody"
		if !openAPIBodyMethods[method] {
			return errors.Errorf("OPENAPI_REQUEST_BODY_ON_%s:%s:DXLIB_READS_A_BODY_FOR_POST_AND_PUT_ONLY", method, rbPointer)
		}
		if op.RequestBody.Content.Len() == 0 {
			return errors.Errorf("OPENAPI_REQUEST_BODY_WITHOUT_CONTENT:%s/content", rbPointer)
		}
		if op.RequestBody.Content.Len() > 1 {
			return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:multiple-media-types:%s/content:AN_ENDPOINT_HAS_ONE_REQUEST_CONTENT_TYPE", rbPointer)
		}
		for _, mediaType := range op.RequestBody.Content.Keys() {
			mtPointer := rbPointer + "/content/" + openAPIPointerEscape(mediaType)
			if _, err := openAPIRequestContentTypeFromName(mediaType, mtPointer); err != nil {
				return err
			}
			mt, _ := op.RequestBody.Content.Get(mediaType)
			if mt.Schema != nil {
				if err := v.schema(mt.Schema, mtPointer+"/schema"); err != nil {
					return err
				}
			}
		}
		if op.RequestContentType != "" {
			return errors.Errorf("OPENAPI_REQUEST_CONTENT_TYPE_CONTRADICTS_REQUEST_BODY:%s/%s", pointer, OpenAPIExtensionRequestContentType)
		}
	}
	if op.RequestContentType != "" {
		if _, err := openAPIRequestContentTypeFromName(op.RequestContentType, pointer+"/"+OpenAPIExtensionRequestContentType); err != nil {
			return err
		}
	}
	if op.ParametersSchema != nil {
		if err := v.schema(op.ParametersSchema, pointer+"/"+OpenAPIExtensionParameters); err != nil {
			return err
		}
		if op.ParametersSchema.Type.Primary() != "object" || op.ParametersSchema.Ref != "" {
			return errors.Errorf("OPENAPI_X_DXLIB_PARAMETERS_MUST_BE_AN_OBJECT_SCHEMA:%s/%s", pointer, OpenAPIExtensionParameters)
		}
	}
	if op.Responses != nil {
		if op.Responses.Len() == 0 {
			return errors.Errorf("OPENAPI_EMPTY_RESPONSES:%s/responses", pointer)
		}
		for _, code := range op.Responses.Keys() {
			rPointer := pointer + "/responses/" + openAPIPointerEscape(code)
			// A response possibility carries an integer status code, so
			// "default" and the 2XX ranges have nothing to bind to.
			n, err := strconv.Atoi(code)
			if err != nil || len(code) != 3 || n < 100 || n > 599 {
				return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:response-code-%s:%s:ONLY_THREE_DIGIT_STATUS_CODES", code, rPointer)
			}
			r, _ := op.Responses.Get(code)
			if r.Headers != nil {
				for _, name := range r.Headers.Keys() {
					h, _ := r.Headers.Get(name)
					if h.Schema != nil {
						if err := v.schema(h.Schema, rPointer+"/headers/"+openAPIPointerEscape(name)+"/schema"); err != nil {
							return err
						}
					}
				}
			}
			if r.Content != nil {
				if r.Content.Len() > 1 {
					return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:multiple-media-types:%s/content", rPointer)
				}
				for _, mediaType := range r.Content.Keys() {
					mtPointer := rPointer + "/content/" + openAPIPointerEscape(mediaType)
					// A DataTemplate is a parameter list, which is a JSON object
					// and nothing else.
					if mediaType != "application/json" {
						return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:response-media-type-%s:%s:ONLY_application~1json", mediaType, mtPointer)
					}
					mt, _ := r.Content.Get(mediaType)
					if mt.Schema == nil {
						return openAPIMissing(mtPointer, "schema")
					}
					if err := v.schema(mt.Schema, mtPointer+"/schema"); err != nil {
						return err
					}
					if mt.Schema.Ref == "" && mt.Schema.Type.Primary() != "object" {
						return errors.Errorf("OPENAPI_RESPONSE_SCHEMA_MUST_BE_AN_OBJECT:%s/schema", mtPointer)
					}
				}
			}
		}
	}
	return nil
}

func (v *openAPIValidator) schema(s *DXOpenAPISchema, pointer string) error {
	if s.Ref != "" {
		name, err := openAPIRefName(s.Ref)
		if err != nil {
			return errors.Wrapf(err, "OPENAPI_AT:%s/$ref", pointer)
		}
		if v.doc.Components == nil || v.doc.Components.Schemas == nil {
			return errors.Errorf("OPENAPI_REF_NOT_FOUND:%s:%s/$ref", s.Ref, pointer)
		}
		if _, ok := v.doc.Components.Schemas.Get(name); !ok {
			return errors.Errorf("OPENAPI_REF_NOT_FOUND:%s:%s/$ref", s.Ref, pointer)
		}
		return nil
	}
	if len(s.Type) == 0 {
		return errors.Errorf("OPENAPI_SCHEMA_WITHOUT_TYPE:%s", pointer)
	}
	nonNull := 0
	for i, t := range s.Type {
		if !openAPIJSONSchemaTypes[t] {
			return errors.Errorf("OPENAPI_UNSUPPORTED_SCHEMA_TYPE:%q:%s/type/%d", t, pointer, i)
		}
		if t != "null" {
			nonNull++
		}
	}
	if nonNull != 1 {
		return errors.Errorf("OPENAPI_UNSUPPORTED_CONSTRUCT:type-union:%s/type:ONE_TYPE_PLUS_OPTIONAL_null", pointer)
	}
	if s.Format != "" && !openAPIKnownFormats[s.Format] {
		return errors.Errorf("OPENAPI_UNSUPPORTED_FORMAT:%q:%s/format", s.Format, pointer)
	}
	if s.DXLibType != "" {
		if _, ok := openAPITypeTable[dxlibTypes.APIParameterType(s.DXLibType)]; !ok {
			return errors.Errorf("OPENAPI_UNKNOWN_X_DXLIB_TYPE:%q:%s/%s", s.DXLibType, pointer, OpenAPIExtensionType)
		}
	}
	if s.Properties != nil {
		for _, name := range s.Properties.Keys() {
			child, _ := s.Properties.Get(name)
			if err := v.schema(child, pointer+"/properties/"+openAPIPointerEscape(name)); err != nil {
				return err
			}
		}
	}
	seen := map[string]bool{}
	for i, name := range s.Required {
		if seen[name] {
			return errors.Errorf("OPENAPI_DUPLICATE_REQUIRED:%s:%s/required/%d", name, pointer, i)
		}
		seen[name] = true
		if s.Properties == nil {
			return errors.Errorf("OPENAPI_REQUIRED_NAMES_UNKNOWN_PROPERTY:%s:%s/required/%d", name, pointer, i)
		}
		if _, ok := s.Properties.Get(name); !ok {
			return errors.Errorf("OPENAPI_REQUIRED_NAMES_UNKNOWN_PROPERTY:%s:%s/required/%d", name, pointer, i)
		}
	}
	if s.Items != nil {
		if err := v.schema(s.Items, pointer+"/items"); err != nil {
			return err
		}
	}
	if s.AdditionalProperties != nil {
		if err := v.schema(s.AdditionalProperties, pointer+"/additionalProperties"); err != nil {
			return err
		}
	}
	return nil
}
