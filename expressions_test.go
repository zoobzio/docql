package docql

import (
	"testing"

	"github.com/zoobzio/docql/internal/types"
)

func TestEq(t *testing.T) {
	field := types.Field{Path: "status", Collection: "users"}
	param := types.Param{Name: "status"}

	cond := Eq(field, param)

	if cond.Field.Path != "status" {
		t.Errorf("Expected field 'status', got '%s'", cond.Field.Path)
	}
	if cond.Operator != types.EQ {
		t.Errorf("Expected EQ operator, got %v", cond.Operator)
	}
	if cond.Value.Name != "status" {
		t.Errorf("Expected value 'status', got '%s'", cond.Value.Name)
	}
}

func TestNe(t *testing.T) {
	field := types.Field{Path: "status"}
	param := types.Param{Name: "status"}

	cond := Ne(field, param)
	if cond.Operator != types.NE {
		t.Errorf("Expected NE operator, got %v", cond.Operator)
	}
}

func TestGt(t *testing.T) {
	field := types.Field{Path: "age"}
	param := types.Param{Name: "minAge"}

	cond := Gt(field, param)
	if cond.Operator != types.GT {
		t.Errorf("Expected GT operator, got %v", cond.Operator)
	}
}

func TestGte(t *testing.T) {
	field := types.Field{Path: "age"}
	param := types.Param{Name: "minAge"}

	cond := Gte(field, param)
	if cond.Operator != types.GTE {
		t.Errorf("Expected GTE operator, got %v", cond.Operator)
	}
}

func TestLt(t *testing.T) {
	field := types.Field{Path: "age"}
	param := types.Param{Name: "maxAge"}

	cond := Lt(field, param)
	if cond.Operator != types.LT {
		t.Errorf("Expected LT operator, got %v", cond.Operator)
	}
}

func TestLte(t *testing.T) {
	field := types.Field{Path: "age"}
	param := types.Param{Name: "maxAge"}

	cond := Lte(field, param)
	if cond.Operator != types.LTE {
		t.Errorf("Expected LTE operator, got %v", cond.Operator)
	}
}

func TestIn(t *testing.T) {
	field := types.Field{Path: "status"}
	param := types.Param{Name: "statuses"}

	cond := In(field, param)
	if cond.Operator != types.IN {
		t.Errorf("Expected IN operator, got %v", cond.Operator)
	}
}

func TestNotIn(t *testing.T) {
	field := types.Field{Path: "status"}
	param := types.Param{Name: "statuses"}

	cond := NotIn(field, param)
	if cond.Operator != types.NotIn {
		t.Errorf("Expected NotIn operator, got %v", cond.Operator)
	}
}

func TestExists(t *testing.T) {
	field := types.Field{Path: "email"}

	filter := Exists(field)
	if !filter.Exists {
		t.Error("Expected Exists to be true")
	}
	if filter.Field.Path != "email" {
		t.Errorf("Expected field 'email', got '%s'", filter.Field.Path)
	}
}

func TestNotExists(t *testing.T) {
	field := types.Field{Path: "email"}

	filter := NotExists(field)
	if filter.Exists {
		t.Error("Expected Exists to be false")
	}
}

func TestRegex(t *testing.T) {
	field := types.Field{Path: "name"}
	pattern := types.Param{Name: "pattern"}

	filter := Regex(field, pattern)
	if filter.Field.Path != "name" {
		t.Errorf("Expected field 'name', got '%s'", filter.Field.Path)
	}
	if filter.Pattern.Name != "pattern" {
		t.Errorf("Expected pattern 'pattern', got '%s'", filter.Pattern.Name)
	}
}

func TestRegexWithOptions(t *testing.T) {
	field := types.Field{Path: "name"}
	pattern := types.Param{Name: "pattern"}
	options := types.Param{Name: "options"}

	filter := RegexWithOptions(field, pattern, options)
	if filter.Options == nil {
		t.Fatal("Expected options to be set")
	}
	if filter.Options.Name != "options" {
		t.Errorf("Expected options 'options', got '%s'", filter.Options.Name)
	}
}

func TestAnd(t *testing.T) {
	cond1 := Eq(types.Field{Path: "a"}, types.Param{Name: "a"})
	cond2 := Eq(types.Field{Path: "b"}, types.Param{Name: "b"})

	group := And(cond1, cond2)
	if group.Logic != types.AND {
		t.Errorf("Expected AND logic, got %v", group.Logic)
	}
	if len(group.Conditions) != 2 {
		t.Errorf("Expected 2 conditions, got %d", len(group.Conditions))
	}
}

func TestOr(t *testing.T) {
	cond1 := Eq(types.Field{Path: "a"}, types.Param{Name: "a"})
	cond2 := Eq(types.Field{Path: "b"}, types.Param{Name: "b"})

	group := Or(cond1, cond2)
	if group.Logic != types.OR {
		t.Errorf("Expected OR logic, got %v", group.Logic)
	}
}

func TestNor(t *testing.T) {
	cond1 := Eq(types.Field{Path: "a"}, types.Param{Name: "a"})
	cond2 := Eq(types.Field{Path: "b"}, types.Param{Name: "b"})

	group := Nor(cond1, cond2)
	if group.Logic != types.NOR {
		t.Errorf("Expected NOR logic, got %v", group.Logic)
	}
}

func TestRange(t *testing.T) {
	field := types.Field{Path: "age"}
	minVal := types.Param{Name: "minAge"}
	maxVal := types.Param{Name: "maxAge"}

	filter := Range(field, &minVal, &maxVal)
	if filter.Field.Path != "age" {
		t.Errorf("Expected field 'age', got '%s'", filter.Field.Path)
	}
	if filter.Min == nil || filter.Min.Name != "minAge" {
		t.Error("Expected Min to be 'minAge'")
	}
	if filter.Max == nil || filter.Max.Name != "maxAge" {
		t.Error("Expected Max to be 'maxAge'")
	}
	if filter.MinExclusive || filter.MaxExclusive {
		t.Error("Expected inclusive bounds by default")
	}
}

func TestRangeExclusive(t *testing.T) {
	field := types.Field{Path: "age"}
	minVal := types.Param{Name: "minAge"}
	maxVal := types.Param{Name: "maxAge"}

	filter := RangeExclusive(field, &minVal, &maxVal)
	if !filter.MinExclusive || !filter.MaxExclusive {
		t.Error("Expected exclusive bounds")
	}
}

func TestGeo(t *testing.T) {
	field := types.Field{Path: "location"}
	lon := types.Param{Name: "lon"}
	lat := types.Param{Name: "lat"}
	radius := types.Param{Name: "radius"}

	filter := Geo(field, lon, lat, radius)
	if filter.Field.Path != "location" {
		t.Errorf("Expected field 'location', got '%s'", filter.Field.Path)
	}
	if filter.Operator != types.Near {
		t.Errorf("Expected Near operator, got %v", filter.Operator)
	}
	if filter.Radius == nil || filter.Radius.Name != "radius" {
		t.Error("Expected radius to be set")
	}
}

func TestAll(t *testing.T) {
	field := types.Field{Path: "tags"}
	param := types.Param{Name: "requiredTags"}

	filter := All(field, param)
	if filter.Operator != types.All {
		t.Errorf("Expected All operator, got %v", filter.Operator)
	}
}

func TestSize(t *testing.T) {
	field := types.Field{Path: "tags"}
	param := types.Param{Name: "size"}

	filter := Size(field, param)
	if filter.Operator != types.Size {
		t.Errorf("Expected Size operator, got %v", filter.Operator)
	}
}

func TestElemMatch(t *testing.T) {
	field := types.Field{Path: "items"}
	cond := Eq(types.Field{Path: "price"}, types.Param{Name: "minPrice"})

	filter := ElemMatch(field, cond)
	if filter.Field.Path != "items" {
		t.Errorf("Expected field 'items', got '%s'", filter.Field.Path)
	}
	if len(filter.Conditions) != 1 {
		t.Errorf("Expected 1 condition, got %d", len(filter.Conditions))
	}
}

func TestTextSearch(t *testing.T) {
	search := types.Param{Name: "searchTerm"}

	filter := TextSearch(search)
	if filter.Search.Name != "searchTerm" {
		t.Errorf("Expected search 'searchTerm', got '%s'", filter.Search.Name)
	}
}

func TestDoc(t *testing.T) {
	builder := Doc()
	if builder == nil {
		t.Fatal("Expected builder, got nil")
	}
}

func TestDocumentBuilder_Set(t *testing.T) {
	field := types.Field{Path: "email"}
	param := types.Param{Name: "email"}

	doc := Doc().Set(field, param).Build()

	if len(doc.Fields) != 1 {
		t.Errorf("Expected 1 field, got %d", len(doc.Fields))
	}
	if doc.Fields[field] != param {
		t.Error("Expected field to be set")
	}
}

func TestDocumentBuilder_ChainedSet(t *testing.T) {
	field1 := types.Field{Path: "email"}
	field2 := types.Field{Path: "name"}
	param1 := types.Param{Name: "email"}
	param2 := types.Param{Name: "name"}

	doc := Doc().
		Set(field1, param1).
		Set(field2, param2).
		Build()

	if len(doc.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(doc.Fields))
	}
}

func TestFieldExpr(t *testing.T) {
	field := types.Field{Path: "price"}

	expr := FieldExpr(field)
	if expr.Field.Path != "price" {
		t.Errorf("Expected field 'price', got '%s'", expr.Field.Path)
	}
}

func TestLiteralExpr(t *testing.T) {
	param := types.Param{Name: "value"}

	expr := LiteralExpr(param)
	if expr.Value.Name != "value" {
		t.Errorf("Expected value 'value', got '%s'", expr.Value.Name)
	}
}

func TestSum(t *testing.T) {
	expr := FieldExpr(types.Field{Path: "amount"})
	acc := Sum(expr)

	if acc.Operator != types.AccSum {
		t.Errorf("Expected AccSum, got %v", acc.Operator)
	}
}

func TestAvg(t *testing.T) {
	expr := FieldExpr(types.Field{Path: "price"})
	acc := Avg(expr)

	if acc.Operator != types.AccAvg {
		t.Errorf("Expected AccAvg, got %v", acc.Operator)
	}
}

func TestMin(t *testing.T) {
	expr := FieldExpr(types.Field{Path: "price"})
	acc := Min(expr)

	if acc.Operator != types.AccMin {
		t.Errorf("Expected AccMin, got %v", acc.Operator)
	}
}

func TestMax(t *testing.T) {
	expr := FieldExpr(types.Field{Path: "price"})
	acc := Max(expr)

	if acc.Operator != types.AccMax {
		t.Errorf("Expected AccMax, got %v", acc.Operator)
	}
}

func TestFirst(t *testing.T) {
	expr := FieldExpr(types.Field{Path: "name"})
	acc := First(expr)

	if acc.Operator != types.AccFirst {
		t.Errorf("Expected AccFirst, got %v", acc.Operator)
	}
}

func TestLast(t *testing.T) {
	expr := FieldExpr(types.Field{Path: "name"})
	acc := Last(expr)

	if acc.Operator != types.AccLast {
		t.Errorf("Expected AccLast, got %v", acc.Operator)
	}
}

func TestCountAcc(t *testing.T) {
	acc := CountAcc()

	if acc.Operator != types.AccCount {
		t.Errorf("Expected AccCount, got %v", acc.Operator)
	}
}
