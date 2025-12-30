// Package docql provides a type-safe query builder for document databases.
//
// DOCQL supports multiple document database backends through a common AST-based
// architecture, similar to how ASTQL handles SQL databases. It provides:
//
//   - Fluent builder API for constructing document queries
//   - Provider-specific renderers for MongoDB, DynamoDB, Firestore, and CouchDB
//   - Schema validation through DDML integration
//   - Parameterized queries for safe query construction
//
// Usage with DDML schema validation:
//
//	import (
//	    "github.com/zoobzio/ddml"
//	    "github.com/zoobzio/docql"
//	    "github.com/zoobzio/docql/pkg/mongodb"
//	)
//
//	schema := ddml.NewSchema("ecommerce").
//	    AddCollection(
//	        ddml.NewCollection("users").
//	            AddField(ddml.NewField("email", ddml.TypeString).WithRequired()).
//	            AddField(ddml.NewField("status", ddml.TypeString)),
//	    )
//
//	d, err := docql.NewFromDDML(schema)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	query := docql.Find(d.C("users")).
//	    Filter(d.Eq(d.F("users", "status"), d.P("status"))).
//	    Limit(10)
//
//	result, err := query.Render(mongodb.New())
package docql

import "github.com/zoobzio/docql/internal/types"

// Renderer defines the interface for provider-specific query rendering.
type Renderer interface {
	// Render converts a DocumentAST to a provider-specific QueryResult.
	Render(ast *types.DocumentAST) (*types.QueryResult, error)

	// SupportsOperation indicates if the provider supports an operation.
	SupportsOperation(op types.Operation) bool

	// SupportsFilter indicates if the provider supports a filter operator.
	SupportsFilter(op types.FilterOperator) bool

	// SupportsUpdate indicates if the provider supports an update operator.
	SupportsUpdate(op types.UpdateOperator) bool

	// SupportsPipelineStage indicates if the provider supports a pipeline stage.
	SupportsPipelineStage(stage string) bool
}

// Re-export output types needed for Renderer interface and results.
// These are OUTPUT types - users receive them, they don't construct them.
type (
	// DocumentAST represents the abstract syntax tree for document queries.
	// Built by the query builder, consumed by renderers.
	DocumentAST = types.DocumentAST

	// QueryResult represents the result of rendering a query.
	// Returned by Renderer.Render().
	QueryResult = types.QueryResult
)

// Re-export interface types for type assertions and polymorphism.
// Users cannot construct these directly - they must use helper functions.
type (
	// FilterItem is the interface for filter conditions.
	// Implemented by FilterCondition, FilterGroup, RangeFilter, etc.
	FilterItem = types.FilterItem

	// PipelineStage is the interface for aggregation pipeline stages.
	PipelineStage = types.PipelineStage

	// Expression is the interface for aggregation expressions.
	Expression = types.Expression

	// Accumulator is returned by Sum(), Avg(), etc. for use in Group().
	// This is an OUTPUT type - users receive it from helper functions.
	Accumulator = types.Accumulator
)

// Re-export enum types - these are safe as they're just type-safe constants.
type (
	// Operation represents a document database operation type.
	Operation = types.Operation

	// FilterOperator represents a filter operator.
	FilterOperator = types.FilterOperator

	// LogicOperator represents a logical operator (AND, OR, NOR).
	LogicOperator = types.LogicOperator

	// UpdateOperator represents an update operator.
	UpdateOperator = types.UpdateOperator

	// SortOrder represents sort direction.
	SortOrder = types.SortOrder
)

// Operation constants.
const (
	OpFind       = types.OpFind
	OpFindOne    = types.OpFindOne
	OpInsert     = types.OpInsert
	OpInsertMany = types.OpInsertMany
	OpUpdate     = types.OpUpdate
	OpUpdateMany = types.OpUpdateMany
	OpDelete     = types.OpDelete
	OpDeleteMany = types.OpDeleteMany
	OpAggregate  = types.OpAggregate
	OpCount      = types.OpCount
	OpDistinct   = types.OpDistinct
)

// Filter operator constants.
const (
	OpEQ            = types.EQ
	OpNE            = types.NE
	OpGT            = types.GT
	OpGTE           = types.GTE
	OpLT            = types.LT
	OpLTE           = types.LTE
	OpIN            = types.IN
	OpNotIn         = types.NotIn
	OpExists        = types.Exists
	OpType          = types.Type
	OpRegex         = types.Regex
	OpText          = types.Text
	OpAll           = types.All
	OpElemMatch     = types.ElemMatch
	OpSize          = types.Size
	OpGeoWithin     = types.GeoWithin
	OpGeoIntersects = types.GeoIntersects
	OpNear          = types.Near
	OpNearSphere    = types.NearSphere
)

// Logic operator constants.
const (
	LogicAND = types.AND
	LogicOR  = types.OR
	LogicNOR = types.NOR
	LogicNOT = types.NOT
)

// Update operator constants.
const (
	UpdateSet         = types.Set
	UpdateUnset       = types.Unset
	UpdateSetOnInsert = types.SetOnInsert
	UpdateInc         = types.Inc
	UpdateMul         = types.Mul
	UpdateMin         = types.Min
	UpdateMax         = types.Max
	UpdateRename      = types.Rename
	UpdateCurrentDate = types.CurrentDate
	UpdateAddToSet    = types.AddToSet
	UpdatePop         = types.Pop
	UpdatePull        = types.Pull
	UpdatePush        = types.Push
	UpdatePullAll     = types.PullAll
)

// Sort order constants.
const (
	Ascending  = types.Ascending
	Descending = types.Descending
)

// Accumulator operator constants.
const (
	AccSum      = types.AccSum
	AccAvg      = types.AccAvg
	AccMin      = types.AccMin
	AccMax      = types.AccMax
	AccFirst    = types.AccFirst
	AccLast     = types.AccLast
	AccPush     = types.AccPush
	AccAddToSet = types.AccAddToSet
	AccCount    = types.AccCount
)

// Complexity limit constants.
const (
	MaxFilterDepth      = types.MaxFilterDepth
	MaxBatchSize        = types.MaxBatchSize
	MaxLimit            = types.MaxLimit
	MaxProjectionFields = types.MaxProjectionFields
	MaxSortFields       = types.MaxSortFields
	MaxPipelineStages   = types.MaxPipelineStages
)
