package types

// Projection represents field selection for query results.
type Projection struct {
	Fields  []ProjectionField
	Exclude bool
}

// ProjectionField represents a single field in a projection.
type ProjectionField struct {
	Field     Field
	Include   bool
	Slice     *SliceOp
	ElemMatch *ElemMatchProjection
}

// SliceOp represents $slice projection for arrays.
type SliceOp struct {
	Count Param
	Skip  *Param
}

// ElemMatchProjection represents $elemMatch in projection.
type ElemMatchProjection struct {
	Conditions []FilterItem
}
