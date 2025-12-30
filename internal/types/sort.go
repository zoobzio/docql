package types

// SortClause represents a single sort specification.
type SortClause struct {
	Field Field
	Order SortOrder
}

// PaginationValue represents a skip or limit value (static or parameterized).
type PaginationValue struct {
	Static *int
	Param  *Param
}
