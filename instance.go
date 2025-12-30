package docql

import (
	"fmt"
	"strings"

	"github.com/zoobzio/ddml"
	"github.com/zoobzio/docql/internal/types"
)

// DOCQL represents an instance with DDML schema validation.
type DOCQL struct {
	schema      *ddml.Schema
	collections map[string]*ddml.Collection
	fields      map[string]map[string]*ddml.Field
	enums       map[string]*ddml.Enum
}

// NewFromDDML creates a new DOCQL instance from a DDML schema.
func NewFromDDML(schema *ddml.Schema) (*DOCQL, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	d := &DOCQL{
		schema:      schema,
		collections: make(map[string]*ddml.Collection),
		fields:      make(map[string]map[string]*ddml.Field),
		enums:       schema.Enums,
	}

	for name, coll := range schema.Collections {
		d.collections[name] = coll
		d.fields[name] = make(map[string]*ddml.Field)
		d.indexFields(name, "", coll.Fields)
	}

	return d, nil
}

func (d *DOCQL) indexFields(collName, prefix string, fields []*ddml.Field) {
	for _, f := range fields {
		path := f.Name
		if prefix != "" {
			path = prefix + "." + f.Name
		}
		d.fields[collName][path] = f

		if f.Type == ddml.TypeObject && len(f.Fields) > 0 {
			d.indexFields(collName, path, f.Fields)
		}

		if f.Type == ddml.TypeArray && f.ArrayOf != nil && f.ArrayOf.Type == ddml.TypeObject {
			d.indexFields(collName, path, f.ArrayOf.Fields)
		}
	}
}

// C creates a validated collection reference.
func (d *DOCQL) C(name string) types.Collection {
	c, err := d.TryC(name)
	if err != nil {
		panic(err)
	}
	return c
}

// TryC creates a collection reference with error handling.
func (d *DOCQL) TryC(name string) (types.Collection, error) {
	if !isValidIdentifier(name) {
		return types.Collection{}, fmt.Errorf("invalid collection name: %s", name)
	}
	if _, ok := d.collections[name]; !ok {
		return types.Collection{}, fmt.Errorf("collection '%s' not found in schema", name)
	}
	return types.Collection{Name: name}, nil
}

// F creates a validated field reference.
func (d *DOCQL) F(collectionName, fieldPath string) types.Field {
	f, err := d.TryF(collectionName, fieldPath)
	if err != nil {
		panic(err)
	}
	return f
}

// TryF creates a field reference with error handling.
func (d *DOCQL) TryF(collectionName, fieldPath string) (types.Field, error) {
	if !isValidFieldPath(fieldPath) {
		return types.Field{}, fmt.Errorf("invalid field path: %s", fieldPath)
	}
	collFields, ok := d.fields[collectionName]
	if !ok {
		return types.Field{}, fmt.Errorf("collection '%s' not found", collectionName)
	}
	if _, ok := collFields[fieldPath]; !ok {
		return types.Field{}, fmt.Errorf("field '%s' not found in collection '%s'",
			fieldPath, collectionName)
	}
	return types.Field{Path: fieldPath, Collection: collectionName}, nil
}

// P creates a validated parameter reference.
func (d *DOCQL) P(name string) types.Param {
	p, err := d.TryP(name)
	if err != nil {
		panic(err)
	}
	return p
}

// TryP creates a parameter with error handling.
func (d *DOCQL) TryP(name string) (types.Param, error) {
	if !isValidIdentifier(name) {
		return types.Param{}, fmt.Errorf("invalid parameter name: %s", name)
	}
	return types.Param{Name: name}, nil
}

// Collections returns all collection names in the schema.
func (d *DOCQL) Collections() []string {
	names := make([]string, 0, len(d.collections))
	for name := range d.collections {
		names = append(names, name)
	}
	return names
}

// Fields returns all field paths for a collection.
func (d *DOCQL) Fields(collectionName string) ([]string, error) {
	collFields, ok := d.fields[collectionName]
	if !ok {
		return nil, fmt.Errorf("collection '%s' not found", collectionName)
	}
	paths := make([]string, 0, len(collFields))
	for path := range collFields {
		paths = append(paths, path)
	}
	return paths, nil
}

// GetFieldType returns the DDML type for a field.
func (d *DOCQL) GetFieldType(collectionName, fieldPath string) (ddml.FieldType, error) {
	if collFields, ok := d.fields[collectionName]; ok {
		if field, ok := collFields[fieldPath]; ok {
			return field.Type, nil
		}
	}
	return "", fmt.Errorf("field '%s' not found in collection '%s'", fieldPath, collectionName)
}

// IsFieldRequired checks if a field is required.
func (d *DOCQL) IsFieldRequired(collectionName, fieldPath string) (bool, error) {
	if collFields, ok := d.fields[collectionName]; ok {
		if field, ok := collFields[fieldPath]; ok {
			return field.Required, nil
		}
	}
	return false, fmt.Errorf("field '%s' not found in collection '%s'", fieldPath, collectionName)
}

// Filter Operator Accessors.

func (*DOCQL) OpEQ() types.FilterOperator            { return types.EQ }
func (*DOCQL) OpNE() types.FilterOperator            { return types.NE }
func (*DOCQL) OpGT() types.FilterOperator            { return types.GT }
func (*DOCQL) OpGTE() types.FilterOperator           { return types.GTE }
func (*DOCQL) OpLT() types.FilterOperator            { return types.LT }
func (*DOCQL) OpLTE() types.FilterOperator           { return types.LTE }
func (*DOCQL) OpIN() types.FilterOperator            { return types.IN }
func (*DOCQL) OpNIN() types.FilterOperator           { return types.NotIn }
func (*DOCQL) OpExists() types.FilterOperator        { return types.Exists }
func (*DOCQL) OpType() types.FilterOperator          { return types.Type }
func (*DOCQL) OpRegex() types.FilterOperator         { return types.Regex }
func (*DOCQL) OpText() types.FilterOperator          { return types.Text }
func (*DOCQL) OpAll() types.FilterOperator           { return types.All }
func (*DOCQL) OpElemMatch() types.FilterOperator     { return types.ElemMatch }
func (*DOCQL) OpSize() types.FilterOperator          { return types.Size }
func (*DOCQL) OpGeoWithin() types.FilterOperator     { return types.GeoWithin }
func (*DOCQL) OpGeoIntersects() types.FilterOperator { return types.GeoIntersects }
func (*DOCQL) OpNear() types.FilterOperator          { return types.Near }
func (*DOCQL) OpNearSphere() types.FilterOperator    { return types.NearSphere }

// Logic Operator Accessors.

func (*DOCQL) LogicAND() types.LogicOperator { return types.AND }
func (*DOCQL) LogicOR() types.LogicOperator  { return types.OR }
func (*DOCQL) LogicNOR() types.LogicOperator { return types.NOR }
func (*DOCQL) LogicNOT() types.LogicOperator { return types.NOT }

// Update Operator Accessors.

func (*DOCQL) UpdateSet() types.UpdateOperator         { return types.Set }
func (*DOCQL) UpdateUnset() types.UpdateOperator       { return types.Unset }
func (*DOCQL) UpdateSetOnInsert() types.UpdateOperator { return types.SetOnInsert }
func (*DOCQL) UpdateInc() types.UpdateOperator         { return types.Inc }
func (*DOCQL) UpdateMul() types.UpdateOperator         { return types.Mul }
func (*DOCQL) UpdateMin() types.UpdateOperator         { return types.Min }
func (*DOCQL) UpdateMax() types.UpdateOperator         { return types.Max }
func (*DOCQL) UpdateRename() types.UpdateOperator      { return types.Rename }
func (*DOCQL) UpdateCurrentDate() types.UpdateOperator { return types.CurrentDate }
func (*DOCQL) UpdateAddToSet() types.UpdateOperator    { return types.AddToSet }
func (*DOCQL) UpdatePop() types.UpdateOperator         { return types.Pop }
func (*DOCQL) UpdatePull() types.UpdateOperator        { return types.Pull }
func (*DOCQL) UpdatePush() types.UpdateOperator        { return types.Push }
func (*DOCQL) UpdatePullAll() types.UpdateOperator     { return types.PullAll }

// Sort Order Accessors.

func (*DOCQL) Asc() types.SortOrder  { return types.Ascending }
func (*DOCQL) Desc() types.SortOrder { return types.Descending }

// Operation Accessors.

func (*DOCQL) OperationFind() types.Operation       { return types.OpFind }
func (*DOCQL) OperationFindOne() types.Operation    { return types.OpFindOne }
func (*DOCQL) OperationInsert() types.Operation     { return types.OpInsert }
func (*DOCQL) OperationInsertMany() types.Operation { return types.OpInsertMany }
func (*DOCQL) OperationUpdate() types.Operation     { return types.OpUpdate }
func (*DOCQL) OperationUpdateMany() types.Operation { return types.OpUpdateMany }
func (*DOCQL) OperationDelete() types.Operation     { return types.OpDelete }
func (*DOCQL) OperationDeleteMany() types.Operation { return types.OpDeleteMany }
func (*DOCQL) OperationAggregate() types.Operation  { return types.OpAggregate }
func (*DOCQL) OperationCount() types.Operation      { return types.OpCount }
func (*DOCQL) OperationDistinct() types.Operation   { return types.OpDistinct }

// Filter Condition Constructors.

func (d *DOCQL) Eq(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.EQ, Value: value}
}

func (d *DOCQL) Ne(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.NE, Value: value}
}

func (d *DOCQL) Gt(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.GT, Value: value}
}

func (d *DOCQL) Gte(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.GTE, Value: value}
}

func (d *DOCQL) Lt(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.LT, Value: value}
}

func (d *DOCQL) Lte(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.LTE, Value: value}
}

func (d *DOCQL) In(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.IN, Value: value}
}

func (d *DOCQL) Nin(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.NotIn, Value: value}
}

func (d *DOCQL) Exists(field types.Field) types.ExistsFilter {
	return types.ExistsFilter{Field: field, Exists: true}
}

func (d *DOCQL) NotExists(field types.Field) types.ExistsFilter {
	return types.ExistsFilter{Field: field, Exists: false}
}

func (d *DOCQL) Regex(field types.Field, pattern types.Param) types.RegexFilter {
	return types.RegexFilter{Field: field, Pattern: pattern}
}

// Filter Group Constructors.

func (d *DOCQL) And(conditions ...types.FilterItem) types.FilterGroup {
	return types.FilterGroup{Logic: types.AND, Conditions: conditions}
}

func (d *DOCQL) TryAnd(conditions ...types.FilterItem) (types.FilterGroup, error) {
	if len(conditions) == 0 {
		return types.FilterGroup{}, fmt.Errorf("AND requires at least one condition")
	}
	return types.FilterGroup{Logic: types.AND, Conditions: conditions}, nil
}

func (d *DOCQL) Or(conditions ...types.FilterItem) types.FilterGroup {
	return types.FilterGroup{Logic: types.OR, Conditions: conditions}
}

func (d *DOCQL) TryOr(conditions ...types.FilterItem) (types.FilterGroup, error) {
	if len(conditions) == 0 {
		return types.FilterGroup{}, fmt.Errorf("OR requires at least one condition")
	}
	return types.FilterGroup{Logic: types.OR, Conditions: conditions}, nil
}

func (d *DOCQL) Nor(conditions ...types.FilterItem) types.FilterGroup {
	return types.FilterGroup{Logic: types.NOR, Conditions: conditions}
}

func (d *DOCQL) TryNor(conditions ...types.FilterItem) (types.FilterGroup, error) {
	if len(conditions) == 0 {
		return types.FilterGroup{}, fmt.Errorf("NOR requires at least one condition")
	}
	return types.FilterGroup{Logic: types.NOR, Conditions: conditions}, nil
}

// Range and Geo Constructors.

func (d *DOCQL) Range(field types.Field, minVal, maxVal *types.Param) types.RangeFilter {
	return types.RangeFilter{Field: field, Min: minVal, Max: maxVal}
}

func (d *DOCQL) TryRange(field types.Field, minVal, maxVal *types.Param) (types.RangeFilter, error) {
	if minVal == nil && maxVal == nil {
		return types.RangeFilter{}, fmt.Errorf("range requires at least min or max")
	}
	return types.RangeFilter{Field: field, Min: minVal, Max: maxVal}, nil
}

func (d *DOCQL) Geo(field types.Field, lon, lat, radius types.Param) types.GeoFilter {
	return types.GeoFilter{
		Field:    field,
		Operator: types.Near,
		Center:   types.GeoPoint{Lon: lon, Lat: lat},
		Radius:   &radius,
	}
}

// Programmatic Helpers.

func (*DOCQL) FilterItems() []types.FilterItem {
	return []types.FilterItem{}
}

func (*DOCQL) Params() []types.Param {
	return []types.Param{}
}

func (*DOCQL) Documents() []types.Document {
	return []types.Document{}
}

func (*DOCQL) Accumulators() map[string]types.Accumulator {
	return make(map[string]types.Accumulator)
}

// Identifier Validation.

var suspiciousPatterns = []string{
	";", "--", "/*", "*/", "'", "\"", "`", "\\",
	" or ", " and ", "drop ", "delete ", "insert ",
	"update ", "select ", "union ", "exec ", "execute ",
}

func isValidIdentifier(s string) bool {
	if s == "" {
		return false
	}

	// Explicit space rejection as defense-in-depth
	if strings.Contains(s, " ") {
		return false
	}

	for i, r := range s {
		if i == 0 {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && r != '_' {
				return false
			}
		} else {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
				return false
			}
		}
	}

	lower := strings.ToLower(s)
	for _, pattern := range suspiciousPatterns {
		if strings.Contains(lower, pattern) {
			return false
		}
	}

	return true
}

func isValidFieldPath(s string) bool {
	if s == "" {
		return false
	}

	// Explicit space rejection as defense-in-depth
	if strings.Contains(s, " ") {
		return false
	}

	parts := strings.Split(s, ".")
	for _, part := range parts {
		if part == "" {
			return false
		}
		for i, r := range part {
			if i == 0 {
				if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && r != '_' && r != '$' {
					return false
				}
			} else {
				if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
					return false
				}
			}
		}
	}

	lower := strings.ToLower(s)
	for _, pattern := range suspiciousPatterns {
		if strings.Contains(lower, pattern) {
			return false
		}
	}

	return true
}
