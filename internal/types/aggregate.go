package types

// PipelineStage represents a single aggregation pipeline stage.
type PipelineStage interface {
	isPipelineStage()
	StageName() string
}

// MatchStage represents $match.
type MatchStage struct {
	Filter FilterItem
}

func (MatchStage) isPipelineStage()  {}
func (MatchStage) StageName() string { return "$match" }

// ProjectStage represents $project.
type ProjectStage struct {
	Projection Projection
	Computed   map[string]Expression
}

func (ProjectStage) isPipelineStage()  {}
func (ProjectStage) StageName() string { return "$project" }

// GroupStage represents $group.
type GroupStage struct {
	ID           Expression
	Accumulators map[string]Accumulator
}

func (GroupStage) isPipelineStage()  {}
func (GroupStage) StageName() string { return "$group" }

// SortStage represents $sort.
type SortStage struct {
	Sorts []SortClause
}

func (SortStage) isPipelineStage()  {}
func (SortStage) StageName() string { return "$sort" }

// LimitStage represents $limit.
type LimitStage struct {
	Limit PaginationValue
}

func (LimitStage) isPipelineStage()  {}
func (LimitStage) StageName() string { return "$limit" }

// SkipStage represents $skip.
type SkipStage struct {
	Skip PaginationValue
}

func (SkipStage) isPipelineStage()  {}
func (SkipStage) StageName() string { return "$skip" }

// UnwindStage represents $unwind.
type UnwindStage struct {
	Path                       Field
	IncludeArrayIndex          *string
	PreserveNullAndEmptyArrays bool
}

func (UnwindStage) isPipelineStage()  {}
func (UnwindStage) StageName() string { return "$unwind" }

// LookupStage represents $lookup.
type LookupStage struct {
	From         string
	LocalField   Field
	ForeignField Field
	As           string
	Pipeline     []PipelineStage
	Let          map[string]Expression
}

func (LookupStage) isPipelineStage()  {}
func (LookupStage) StageName() string { return "$lookup" }

// AddFieldsStage represents $addFields.
type AddFieldsStage struct {
	Fields map[string]Expression
}

func (AddFieldsStage) isPipelineStage()  {}
func (AddFieldsStage) StageName() string { return "$addFields" }

// ReplaceRootStage represents $replaceRoot.
type ReplaceRootStage struct {
	NewRoot Expression
}

func (ReplaceRootStage) isPipelineStage()  {}
func (ReplaceRootStage) StageName() string { return "$replaceRoot" }

// CountStage represents $count.
type CountStage struct {
	FieldName string
}

func (CountStage) isPipelineStage()  {}
func (CountStage) StageName() string { return "$count" }

// FacetStage represents $facet.
type FacetStage struct {
	Facets map[string][]PipelineStage
}

func (FacetStage) isPipelineStage()  {}
func (FacetStage) StageName() string { return "$facet" }

// BucketStage represents $bucket.
type BucketStage struct {
	GroupBy    Expression
	Boundaries []Param
	Default    *Param
	Output     map[string]Accumulator
}

func (BucketStage) isPipelineStage()  {}
func (BucketStage) StageName() string { return "$bucket" }

// Expression represents an aggregation expression.
type Expression interface {
	isExpression()
}

// FieldExpression references a document field.
type FieldExpression struct {
	Field Field
}

func (FieldExpression) isExpression() {}

// LiteralExpression represents a literal value.
type LiteralExpression struct {
	Value Param
}

func (LiteralExpression) isExpression() {}

// OperatorExpression represents an operator expression.
type OperatorExpression struct {
	Operator string
	Args     []Expression
}

func (OperatorExpression) isExpression() {}

// ConditionalExpression represents $cond.
type ConditionalExpression struct {
	If   Expression
	Then Expression
	Else Expression
}

func (ConditionalExpression) isExpression() {}

// Accumulator represents a group accumulator.
type Accumulator struct {
	Operator string
	Expr     Expression
}

// Accumulator operator constants.
const (
	AccSum      = "$sum"
	AccAvg      = "$avg"
	AccMin      = "$min"
	AccMax      = "$max"
	AccFirst    = "$first"
	AccLast     = "$last"
	AccPush     = "$push"
	AccAddToSet = "$addToSet"
	AccCount    = "$count"
)
