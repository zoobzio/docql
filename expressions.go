package docql

import "github.com/zoobzio/docql/internal/types"

// Eq creates an equality filter condition.
func Eq(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.EQ, Value: value}
}

// Ne creates a not-equal filter condition.
func Ne(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.NE, Value: value}
}

// Gt creates a greater-than filter condition.
func Gt(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.GT, Value: value}
}

// Gte creates a greater-than-or-equal filter condition.
func Gte(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.GTE, Value: value}
}

// Lt creates a less-than filter condition.
func Lt(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.LT, Value: value}
}

// Lte creates a less-than-or-equal filter condition.
func Lte(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.LTE, Value: value}
}

// In creates an IN filter condition.
func In(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.IN, Value: value}
}

// NotIn creates a NOT IN filter condition.
func NotIn(field types.Field, value types.Param) types.FilterCondition {
	return types.FilterCondition{Field: field, Operator: types.NotIn, Value: value}
}

// Exists creates a field existence filter.
func Exists(field types.Field) types.ExistsFilter {
	return types.ExistsFilter{Field: field, Exists: true}
}

// NotExists creates a field non-existence filter.
func NotExists(field types.Field) types.ExistsFilter {
	return types.ExistsFilter{Field: field, Exists: false}
}

// Regex creates a regex filter.
func Regex(field types.Field, pattern types.Param) types.RegexFilter {
	return types.RegexFilter{Field: field, Pattern: pattern}
}

// RegexWithOptions creates a regex filter with options.
func RegexWithOptions(field types.Field, pattern, options types.Param) types.RegexFilter {
	return types.RegexFilter{Field: field, Pattern: pattern, Options: &options}
}

// And creates an AND filter group.
func And(conditions ...types.FilterItem) types.FilterGroup {
	return types.FilterGroup{Logic: types.AND, Conditions: conditions}
}

// Or creates an OR filter group.
func Or(conditions ...types.FilterItem) types.FilterGroup {
	return types.FilterGroup{Logic: types.OR, Conditions: conditions}
}

// Nor creates a NOR filter group.
func Nor(conditions ...types.FilterItem) types.FilterGroup {
	return types.FilterGroup{Logic: types.NOR, Conditions: conditions}
}

// Range creates a range filter.
func Range(field types.Field, minVal, maxVal *types.Param) types.RangeFilter {
	return types.RangeFilter{Field: field, Min: minVal, Max: maxVal}
}

// RangeExclusive creates a range filter with exclusive bounds.
func RangeExclusive(field types.Field, minVal, maxVal *types.Param) types.RangeFilter {
	return types.RangeFilter{
		Field:        field,
		Min:          minVal,
		Max:          maxVal,
		MinExclusive: true,
		MaxExclusive: true,
	}
}

// Geo creates a geospatial filter.
func Geo(field types.Field, lon, lat, radius types.Param) types.GeoFilter {
	return types.GeoFilter{
		Field:    field,
		Operator: types.Near,
		Center:   types.GeoPoint{Lon: lon, Lat: lat},
		Radius:   &radius,
	}
}

// All creates an $all array filter.
func All(field types.Field, value types.Param) types.ArrayFilter {
	return types.ArrayFilter{Field: field, Operator: types.All, Value: value}
}

// Size creates a $size array filter.
func Size(field types.Field, value types.Param) types.ArrayFilter {
	return types.ArrayFilter{Field: field, Operator: types.Size, Value: value}
}

// ElemMatch creates an $elemMatch filter.
func ElemMatch(field types.Field, conditions ...types.FilterItem) types.ElemMatchFilter {
	return types.ElemMatchFilter{Field: field, Conditions: conditions}
}

// TextSearch creates a text search filter.
func TextSearch(search types.Param) types.TextSearchFilter {
	return types.TextSearchFilter{Search: search}
}

// Doc creates a new document for insert operations.
func Doc() *DocumentBuilder {
	return &DocumentBuilder{
		doc: types.Document{
			Fields: make(map[types.Field]types.Param),
		},
	}
}

// DocumentBuilder builds documents for insert operations.
type DocumentBuilder struct {
	doc types.Document
}

// Set adds a field to the document.
func (db *DocumentBuilder) Set(field types.Field, value types.Param) *DocumentBuilder {
	db.doc.Fields[field] = value
	return db
}

// Build returns the document.
func (db *DocumentBuilder) Build() types.Document {
	return db.doc
}

// FieldExpr creates a field expression for aggregations.
func FieldExpr(field types.Field) types.FieldExpression {
	return types.FieldExpression{Field: field}
}

// LiteralExpr creates a literal expression for aggregations.
func LiteralExpr(value types.Param) types.LiteralExpression {
	return types.LiteralExpression{Value: value}
}

// Sum creates a $sum accumulator.
func Sum(expr types.Expression) types.Accumulator {
	return types.Accumulator{Operator: types.AccSum, Expr: expr}
}

// Avg creates an $avg accumulator.
func Avg(expr types.Expression) types.Accumulator {
	return types.Accumulator{Operator: types.AccAvg, Expr: expr}
}

// Min creates a $min accumulator.
func Min(expr types.Expression) types.Accumulator {
	return types.Accumulator{Operator: types.AccMin, Expr: expr}
}

// Max creates a $max accumulator.
func Max(expr types.Expression) types.Accumulator {
	return types.Accumulator{Operator: types.AccMax, Expr: expr}
}

// First creates a $first accumulator.
func First(expr types.Expression) types.Accumulator {
	return types.Accumulator{Operator: types.AccFirst, Expr: expr}
}

// Last creates a $last accumulator.
func Last(expr types.Expression) types.Accumulator {
	return types.Accumulator{Operator: types.AccLast, Expr: expr}
}

// CountAcc creates a $count accumulator.
func CountAcc() types.Accumulator {
	return types.Accumulator{Operator: types.AccCount}
}
