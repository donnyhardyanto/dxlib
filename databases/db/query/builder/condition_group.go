package builder

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/utils"
)

// ConditionGroup builds a set of AND-joined conditions with unique named parameters.
// Multiple ConditionGroups can be OR'd together via SelectQueryBuilder.OrGroups.
type ConditionGroup struct {
	Conditions []string
	Args       utils.JSON
	prefix     string
	counter    int
}

func NewConditionGroup(prefix string) *ConditionGroup {
	return &ConditionGroup{
		Conditions: []string{},
		Args:       utils.JSON{},
		prefix:     prefix,
	}
}

func (cg *ConditionGroup) nextParam(hint string) string {
	name := fmt.Sprintf("%s_%s_%d", cg.prefix, hint, cg.counter)
	cg.counter++
	return name
}

// InInt64 adds field IN (1, 2, 3) — literal integers, no named params needed.
func (cg *ConditionGroup) InInt64(field string, values []int64) *ConditionGroup {
	if len(values) == 0 {
		return cg
	}
	var parts []string
	for _, v := range values {
		parts = append(parts, fmt.Sprintf("%d", v))
	}
	cg.Conditions = append(cg.Conditions, fmt.Sprintf("%s IN (%s)", field, strings.Join(parts, ", ")))
	return cg
}

// InStrings adds field IN (:param0, :param1, ...) with named parameters.
func (cg *ConditionGroup) InStrings(field string, values []string) *ConditionGroup {
	if len(values) == 0 {
		return cg
	}
	var paramRefs []string
	for _, v := range values {
		param := cg.nextParam(field)
		paramRefs = append(paramRefs, ":"+param)
		cg.Args[param] = v
	}
	cg.Conditions = append(cg.Conditions, fmt.Sprintf("%s IN (%s)", field, strings.Join(paramRefs, ", ")))
	return cg
}

// OrInStrings adds (f1 IN (:p0,:p1) OR f2 IN (:p0,:p1)) with shared named parameters.
func (cg *ConditionGroup) OrInStrings(fields []string, values []string) *ConditionGroup {
	if len(values) == 0 || len(fields) == 0 {
		return cg
	}
	var paramRefs []string
	for _, v := range values {
		param := cg.nextParam("loc")
		paramRefs = append(paramRefs, ":"+param)
		cg.Args[param] = v
	}
	inList := strings.Join(paramRefs, ", ")
	var parts []string
	for _, f := range fields {
		parts = append(parts, fmt.Sprintf("%s IN (%s)", f, inList))
	}
	cg.Conditions = append(cg.Conditions, "("+strings.Join(parts, " OR ")+")")
	return cg
}

// Eq adds field = :param with a named parameter.
func (cg *ConditionGroup) Eq(field string, value any) *ConditionGroup {
	param := cg.nextParam(field)
	cg.Conditions = append(cg.Conditions, fmt.Sprintf("%s = :%s", field, param))
	cg.Args[param] = value
	return cg
}

// SearchLike adds (LOWER(f1) LIKE LOWER(:arg) OR LOWER(f2) LIKE LOWER(:arg)) with a single named parameter.
func (cg *ConditionGroup) SearchLike(value string, fields ...string) *ConditionGroup {
	if value == "" || len(fields) == 0 {
		return cg
	}
	param := cg.nextParam("search")
	var parts []string
	for _, f := range fields {
		parts = append(parts, fmt.Sprintf("LOWER(%s) LIKE LOWER(:%s)", f, param))
	}
	cg.Conditions = append(cg.Conditions, "("+strings.Join(parts, " OR ")+")")
	cg.Args[param] = "%" + value + "%"
	return cg
}

// And adds a raw condition string.
func (cg *ConditionGroup) And(raw string) *ConditionGroup {
	if raw != "" {
		cg.Conditions = append(cg.Conditions, raw)
	}
	return cg
}

// Build returns the AND-joined conditions string.
func (cg *ConditionGroup) Build() string {
	if len(cg.Conditions) == 0 {
		return ""
	}
	return strings.Join(cg.Conditions, " AND ")
}
