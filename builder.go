package docql

import (
	"fmt"

	"github.com/zoobzio/docql/internal/types"
)

// Builder provides a fluent API for constructing document queries.
type Builder struct {
	ast *types.DocumentAST
	err error
}

// Find creates a new find query builder.
func Find(c types.Collection) *Builder {
	return &Builder{
		ast: &types.DocumentAST{
			Operation: types.OpFind,
			Target:    c,
		},
	}
}

// FindOne creates a find-one query builder.
func FindOne(c types.Collection) *Builder {
	return &Builder{
		ast: &types.DocumentAST{
			Operation: types.OpFindOne,
			Target:    c,
		},
	}
}

// Insert creates an insert query builder.
func Insert(c types.Collection) *Builder {
	return &Builder{
		ast: &types.DocumentAST{
			Operation: types.OpInsert,
			Target:    c,
			Documents: make([]types.Document, 0, 1),
		},
	}
}

// InsertMany creates a batch insert query builder.
func InsertMany(c types.Collection) *Builder {
	return &Builder{
		ast: &types.DocumentAST{
			Operation: types.OpInsertMany,
			Target:    c,
			Documents: make([]types.Document, 0),
		},
	}
}

// Update creates an update query builder.
func Update(c types.Collection) *Builder {
	return &Builder{
		ast: &types.DocumentAST{
			Operation: types.OpUpdate,
			Target:    c,
			UpdateOps: make([]types.UpdateOperation, 0),
		},
	}
}

// UpdateMany creates a batch update query builder.
func UpdateMany(c types.Collection) *Builder {
	return &Builder{
		ast: &types.DocumentAST{
			Operation: types.OpUpdateMany,
			Target:    c,
			UpdateOps: make([]types.UpdateOperation, 0),
		},
	}
}

// Delete creates a delete query builder.
func Delete(c types.Collection) *Builder {
	return &Builder{
		ast: &types.DocumentAST{
			Operation: types.OpDelete,
			Target:    c,
		},
	}
}

// DeleteMany creates a batch delete query builder.
func DeleteMany(c types.Collection) *Builder {
	return &Builder{
		ast: &types.DocumentAST{
			Operation: types.OpDeleteMany,
			Target:    c,
		},
	}
}

// Aggregate creates an aggregation pipeline builder.
func Aggregate(c types.Collection) *Builder {
	return &Builder{
		ast: &types.DocumentAST{
			Operation: types.OpAggregate,
			Target:    c,
			Pipeline:  make([]types.PipelineStage, 0),
		},
	}
}

// Count creates a count query builder.
func Count(c types.Collection) *Builder {
	return &Builder{
		ast: &types.DocumentAST{
			Operation: types.OpCount,
			Target:    c,
		},
	}
}

// Distinct creates a distinct query builder.
func Distinct(c types.Collection, field types.Field) *Builder {
	return &Builder{
		ast: &types.DocumentAST{
			Operation:     types.OpDistinct,
			Target:        c,
			DistinctField: &field,
		},
	}
}

// Filter sets or adds to the filter clause.
func (b *Builder) Filter(f types.FilterItem) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.FilterClause == nil {
		b.ast.FilterClause = f
	} else {
		b.ast.FilterClause = types.FilterGroup{
			Logic:      types.AND,
			Conditions: []types.FilterItem{b.ast.FilterClause, f},
		}
	}
	return b
}

// Where is an alias for Filter.
func (b *Builder) Where(f types.FilterItem) *Builder {
	return b.Filter(f)
}

// Select specifies fields to include in results.
func (b *Builder) Select(fields ...types.Field) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isReadOperation() {
		b.err = fmt.Errorf("Select() can only be used with read operations")
		return b
	}
	projFields := make([]types.ProjectionField, len(fields))
	for i, f := range fields {
		projFields[i] = types.ProjectionField{Field: f, Include: true}
	}
	b.ast.Projection = &types.Projection{Fields: projFields, Exclude: false}
	return b
}

// Exclude specifies fields to exclude from results.
func (b *Builder) Exclude(fields ...types.Field) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isReadOperation() {
		b.err = fmt.Errorf("Exclude() can only be used with read operations")
		return b
	}
	projFields := make([]types.ProjectionField, len(fields))
	for i, f := range fields {
		projFields[i] = types.ProjectionField{Field: f, Include: false}
	}
	b.ast.Projection = &types.Projection{Fields: projFields, Exclude: true}
	return b
}

// Sort adds a sort clause.
func (b *Builder) Sort(field types.Field, order types.SortOrder) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isReadOperation() {
		b.err = fmt.Errorf("Sort() can only be used with read operations")
		return b
	}
	b.ast.SortClauses = append(b.ast.SortClauses, types.SortClause{
		Field: field,
		Order: order,
	})
	return b
}

// SortAsc adds ascending sort.
func (b *Builder) SortAsc(field types.Field) *Builder {
	return b.Sort(field, types.Ascending)
}

// SortDesc adds descending sort.
func (b *Builder) SortDesc(field types.Field) *Builder {
	return b.Sort(field, types.Descending)
}

// Skip sets the number of documents to skip.
func (b *Builder) Skip(n int) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isReadOperation() {
		b.err = fmt.Errorf("Skip() can only be used with read operations")
		return b
	}
	b.ast.Skip = &types.PaginationValue{Static: &n}
	return b
}

// SkipParam sets skip from a parameter.
func (b *Builder) SkipParam(p types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isReadOperation() {
		b.err = fmt.Errorf("SkipParam() can only be used with read operations")
		return b
	}
	b.ast.Skip = &types.PaginationValue{Param: &p}
	return b
}

// Limit sets the maximum number of documents to return.
func (b *Builder) Limit(n int) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isReadOperation() {
		b.err = fmt.Errorf("Limit() can only be used with read operations")
		return b
	}
	if n > types.MaxLimit {
		b.err = fmt.Errorf("limit exceeds maximum: %d > %d", n, types.MaxLimit)
		return b
	}
	b.ast.Limit = &types.PaginationValue{Static: &n}
	return b
}

// LimitParam sets limit from a parameter.
func (b *Builder) LimitParam(p types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isReadOperation() {
		b.err = fmt.Errorf("LimitParam() can only be used with read operations")
		return b
	}
	b.ast.Limit = &types.PaginationValue{Param: &p}
	return b
}

// Document adds a document for insert.
func (b *Builder) Document(doc types.Document) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpInsert && b.ast.Operation != types.OpInsertMany {
		b.err = fmt.Errorf("Document() can only be used with INSERT operations")
		return b
	}
	b.ast.Documents = append(b.ast.Documents, doc)
	return b
}

// Documents adds multiple documents for batch insert.
func (b *Builder) Documents(docs []types.Document) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpInsertMany {
		b.err = fmt.Errorf("Documents() can only be used with INSERT_MANY")
		return b
	}
	b.ast.Documents = append(b.ast.Documents, docs...)
	return b
}

// Set adds a $set update operation.
func (b *Builder) Set(field types.Field, value types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isUpdateOperation() {
		b.err = fmt.Errorf("Set() can only be used with UPDATE operations")
		return b
	}
	b.addOrMergeUpdate(types.Set, field, value)
	return b
}

// Unset adds an $unset operation.
func (b *Builder) Unset(fields ...types.Field) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isUpdateOperation() {
		b.err = fmt.Errorf("Unset() can only be used with UPDATE operations")
		return b
	}
	for _, f := range fields {
		b.addOrMergeUpdate(types.Unset, f, types.Param{})
	}
	return b
}

// Inc adds an $inc operation.
func (b *Builder) Inc(field types.Field, value types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isUpdateOperation() {
		b.err = fmt.Errorf("Inc() can only be used with UPDATE operations")
		return b
	}
	b.addOrMergeUpdate(types.Inc, field, value)
	return b
}

// Mul adds a $mul operation.
func (b *Builder) Mul(field types.Field, value types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isUpdateOperation() {
		b.err = fmt.Errorf("Mul() can only be used with UPDATE operations")
		return b
	}
	b.addOrMergeUpdate(types.Mul, field, value)
	return b
}

// Push adds a $push operation.
func (b *Builder) Push(field types.Field, value types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isUpdateOperation() {
		b.err = fmt.Errorf("Push() can only be used with UPDATE operations")
		return b
	}
	b.addOrMergeUpdate(types.Push, field, value)
	return b
}

// Pull adds a $pull operation.
func (b *Builder) Pull(field types.Field, value types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isUpdateOperation() {
		b.err = fmt.Errorf("Pull() can only be used with UPDATE operations")
		return b
	}
	b.addOrMergeUpdate(types.Pull, field, value)
	return b
}

// AddToSet adds an $addToSet operation.
func (b *Builder) AddToSet(field types.Field, value types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if !b.isUpdateOperation() {
		b.err = fmt.Errorf("AddToSet() can only be used with UPDATE operations")
		return b
	}
	b.addOrMergeUpdate(types.AddToSet, field, value)
	return b
}

// Upsert enables upsert mode.
func (b *Builder) Upsert() *Builder {
	if b.err != nil {
		return b
	}
	if !b.isUpdateOperation() {
		b.err = fmt.Errorf("Upsert() can only be used with UPDATE operations")
		return b
	}
	b.ast.Upsert = true
	return b
}

// Match adds a $match pipeline stage.
func (b *Builder) Match(filter types.FilterItem) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpAggregate {
		b.err = fmt.Errorf("Match() can only be used with AGGREGATE")
		return b
	}
	b.ast.Pipeline = append(b.ast.Pipeline, types.MatchStage{Filter: filter})
	return b
}

// Project adds a $project pipeline stage.
func (b *Builder) Project(proj types.Projection) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpAggregate {
		b.err = fmt.Errorf("Project() can only be used with AGGREGATE")
		return b
	}
	b.ast.Pipeline = append(b.ast.Pipeline, types.ProjectStage{Projection: proj})
	return b
}

// Group adds a $group pipeline stage.
func (b *Builder) Group(id types.Expression, accumulators map[string]types.Accumulator) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpAggregate {
		b.err = fmt.Errorf("Group() can only be used with AGGREGATE")
		return b
	}
	b.ast.Pipeline = append(b.ast.Pipeline, types.GroupStage{
		ID:           id,
		Accumulators: accumulators,
	})
	return b
}

// Lookup adds a $lookup pipeline stage.
func (b *Builder) Lookup(from string, localField, foreignField types.Field, as string) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpAggregate {
		b.err = fmt.Errorf("Lookup() can only be used with AGGREGATE")
		return b
	}
	b.ast.Pipeline = append(b.ast.Pipeline, types.LookupStage{
		From:         from,
		LocalField:   localField,
		ForeignField: foreignField,
		As:           as,
	})
	return b
}

// Unwind adds an $unwind pipeline stage.
func (b *Builder) Unwind(path types.Field) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpAggregate {
		b.err = fmt.Errorf("Unwind() can only be used with AGGREGATE")
		return b
	}
	b.ast.Pipeline = append(b.ast.Pipeline, types.UnwindStage{Path: path})
	return b
}

// Stage adds a custom pipeline stage.
func (b *Builder) Stage(stage types.PipelineStage) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpAggregate {
		b.err = fmt.Errorf("Stage() can only be used with AGGREGATE")
		return b
	}
	b.ast.Pipeline = append(b.ast.Pipeline, stage)
	return b
}

// Build returns the constructed AST or an error.
func (b *Builder) Build() (*types.DocumentAST, error) {
	if b.err != nil {
		return nil, b.err
	}
	if err := b.ast.Validate(); err != nil {
		return nil, err
	}
	return b.ast, nil
}

// MustBuild returns the AST or panics on error.
func (b *Builder) MustBuild() *types.DocumentAST {
	ast, err := b.Build()
	if err != nil {
		panic(err)
	}
	return ast
}

// Render builds the AST and renders it using the provided renderer.
func (b *Builder) Render(renderer Renderer) (*types.QueryResult, error) {
	ast, err := b.Build()
	if err != nil {
		return nil, err
	}
	return renderer.Render(ast)
}

// MustRender renders the query or panics on error.
func (b *Builder) MustRender(renderer Renderer) *types.QueryResult {
	result, err := b.Render(renderer)
	if err != nil {
		panic(err)
	}
	return result
}

func (b *Builder) isReadOperation() bool {
	return b.ast.Operation == types.OpFind ||
		b.ast.Operation == types.OpFindOne ||
		b.ast.Operation == types.OpAggregate ||
		b.ast.Operation == types.OpCount ||
		b.ast.Operation == types.OpDistinct
}

func (b *Builder) isUpdateOperation() bool {
	return b.ast.Operation == types.OpUpdate ||
		b.ast.Operation == types.OpUpdateMany
}

func (b *Builder) addOrMergeUpdate(op types.UpdateOperator, field types.Field, value types.Param) {
	for i, existing := range b.ast.UpdateOps {
		if existing.Operator == op {
			b.ast.UpdateOps[i].Fields[field] = value
			return
		}
	}
	b.ast.UpdateOps = append(b.ast.UpdateOps, types.UpdateOperation{
		Operator: op,
		Fields:   map[types.Field]types.Param{field: value},
	})
}
