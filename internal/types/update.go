package types

// UpdateOperation represents a single update operation.
type UpdateOperation struct {
	Operator UpdateOperator
	Fields   map[Field]Param
}

// ArrayUpdateOperation represents array-specific updates with modifiers.
type ArrayUpdateOperation struct {
	Operator  UpdateOperator
	Field     Field
	Value     Param
	Modifiers *ArrayModifiers
}

// ArrayModifiers represents modifiers for $push operations.
type ArrayModifiers struct {
	Each     []Param
	Position *Param
	Slice    *Param
	Sort     []SortClause
}

// Document represents a document for insert operations.
type Document struct {
	Fields map[Field]Param
}
