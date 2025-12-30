// Package benchmarks provides performance benchmarks for docql.
package benchmarks

import (
	"testing"

	"github.com/zoobzio/ddml"
	"github.com/zoobzio/docql"
	"github.com/zoobzio/docql/pkg/mongodb"
)

func createBenchmarkInstance(b *testing.B) *docql.DOCQL {
	b.Helper()

	schema := ddml.NewSchema("bench").
		AddCollection(
			ddml.NewCollection("users").
				AddField(ddml.NewField("_id", ddml.TypeString)).
				AddField(ddml.NewField("username", ddml.TypeString)).
				AddField(ddml.NewField("email", ddml.TypeString)).
				AddField(ddml.NewField("age", ddml.TypeInt)).
				AddField(ddml.NewField("active", ddml.TypeBool)).
				AddField(ddml.NewField("createdAt", ddml.TypeDate)),
		).
		AddCollection(
			ddml.NewCollection("posts").
				AddField(ddml.NewField("_id", ddml.TypeString)).
				AddField(ddml.NewField("userId", ddml.TypeString)).
				AddField(ddml.NewField("title", ddml.TypeString)).
				AddField(ddml.NewField("views", ddml.TypeInt)).
				AddField(ddml.NewField("published", ddml.TypeBool)),
		).
		AddCollection(
			ddml.NewCollection("orders").
				AddField(ddml.NewField("_id", ddml.TypeString)).
				AddField(ddml.NewField("userId", ddml.TypeString)).
				AddField(ddml.NewField("total", ddml.TypeFloat)).
				AddField(ddml.NewField("status", ddml.TypeString)),
		)

	instance, err := docql.NewFromDDML(schema)
	if err != nil {
		b.Fatalf("Failed to create instance: %v", err)
	}
	return instance
}

// BenchmarkSimpleFind measures simple find query rendering.
func BenchmarkSimpleFind(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("users")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := docql.Find(collection).Render(mongodb.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFindWithFilter measures find with filter query rendering.
func BenchmarkFindWithFilter(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("users")
	filter := instance.Eq(instance.F("users", "active"), instance.P("active"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := docql.Find(collection).Filter(filter).Render(mongodb.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFindWithComplexFilter measures find with complex AND/OR filter.
func BenchmarkFindWithComplexFilter(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("users")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := docql.Find(collection).
			Filter(instance.And(
				instance.Eq(instance.F("users", "active"), instance.P("active")),
				instance.Or(
					instance.Gt(instance.F("users", "age"), instance.P("minAge")),
					instance.Eq(instance.F("users", "username"), instance.P("username")),
				),
			)).
			Render(mongodb.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFindWithProjection measures find with field projection.
func BenchmarkFindWithProjection(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("users")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := docql.Find(collection).
			Select(
				instance.F("users", "_id"),
				instance.F("users", "username"),
				instance.F("users", "email"),
				instance.F("users", "age"),
			).
			Render(mongodb.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFindWithSort measures find with sort clause.
func BenchmarkFindWithSort(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("users")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := docql.Find(collection).
			SortDesc(instance.F("users", "createdAt")).
			Render(mongodb.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFindWithPagination measures find with limit and skip.
func BenchmarkFindWithPagination(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("users")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := docql.Find(collection).
			Limit(10).
			Skip(20).
			Render(mongodb.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFindComplete measures a complete find query with all options.
func BenchmarkFindComplete(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("users")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := docql.Find(collection).
			Filter(instance.Eq(instance.F("users", "active"), instance.P("active"))).
			Select(
				instance.F("users", "_id"),
				instance.F("users", "username"),
				instance.F("users", "email"),
			).
			SortDesc(instance.F("users", "createdAt")).
			Limit(10).
			Skip(0).
			Render(mongodb.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInsert measures insert query rendering.
func BenchmarkInsert(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("users")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		doc := docql.Doc().
			Set(instance.F("users", "username"), instance.P("username")).
			Set(instance.F("users", "email"), instance.P("email")).
			Set(instance.F("users", "age"), instance.P("age")).
			Build()

		_, err := docql.Insert(collection).Document(doc).Render(mongodb.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUpdate measures update query rendering.
func BenchmarkUpdate(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("users")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := docql.Update(collection).
			Filter(instance.Eq(instance.F("users", "_id"), instance.P("id"))).
			Set(instance.F("users", "username"), instance.P("newUsername")).
			Set(instance.F("users", "email"), instance.P("newEmail")).
			Render(mongodb.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDelete measures delete query rendering.
func BenchmarkDelete(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("users")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := docql.Delete(collection).
			Filter(instance.Eq(instance.F("users", "_id"), instance.P("id"))).
			Render(mongodb.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCount measures count query rendering.
func BenchmarkCount(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("users")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := docql.Count(collection).
			Filter(instance.Eq(instance.F("users", "active"), instance.P("active"))).
			Render(mongodb.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAggregate measures aggregation pipeline rendering.
func BenchmarkAggregate(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("orders")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		accumulators := map[string]docql.Accumulator{
			"totalSum": docql.Sum(docql.FieldExpr(instance.F("orders", "total"))),
		}
		_, err := docql.Aggregate(collection).
			Match(instance.Eq(instance.F("orders", "status"), instance.P("status"))).
			Group(docql.FieldExpr(instance.F("orders", "userId")), accumulators).
			Render(mongodb.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAggregateComplex measures complex aggregation pipeline.
func BenchmarkAggregateComplex(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("orders")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		accumulators := map[string]docql.Accumulator{
			"totalSum":   docql.Sum(docql.FieldExpr(instance.F("orders", "total"))),
			"totalAvg":   docql.Avg(docql.FieldExpr(instance.F("orders", "total"))),
			"orderCount": docql.CountAcc(),
		}
		_, err := docql.Aggregate(collection).
			Match(instance.Eq(instance.F("orders", "status"), instance.P("status"))).
			Group(docql.FieldExpr(instance.F("orders", "userId")), accumulators).
			Sort(instance.F("orders", "total"), docql.Descending).
			Limit(10).
			Render(mongodb.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark component creation (not rendering).

// BenchmarkCollectionCreation measures collection reference creation overhead.
func BenchmarkCollectionCreation(b *testing.B) {
	instance := createBenchmarkInstance(b)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = instance.C("users")
	}
}

// BenchmarkFieldCreation measures field reference creation overhead.
func BenchmarkFieldCreation(b *testing.B) {
	instance := createBenchmarkInstance(b)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = instance.F("users", "username")
	}
}

// BenchmarkParamCreation measures parameter creation overhead.
func BenchmarkParamCreation(b *testing.B) {
	instance := createBenchmarkInstance(b)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = instance.P("user_id")
	}
}

// BenchmarkFilterCreation measures filter condition creation overhead.
func BenchmarkFilterCreation(b *testing.B) {
	instance := createBenchmarkInstance(b)
	field := instance.F("users", "active")
	param := instance.P("is_active")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = instance.Eq(field, param)
	}
}

// BenchmarkAndGroupCreation measures AND group creation overhead.
func BenchmarkAndGroupCreation(b *testing.B) {
	instance := createBenchmarkInstance(b)
	cond1 := instance.Eq(instance.F("users", "active"), instance.P("active"))
	cond2 := instance.Gt(instance.F("users", "age"), instance.P("age"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = instance.And(cond1, cond2)
	}
}

// BenchmarkDocumentBuilding measures document builder overhead.
func BenchmarkDocumentBuilding(b *testing.B) {
	instance := createBenchmarkInstance(b)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = docql.Doc().
			Set(instance.F("users", "username"), instance.P("username")).
			Set(instance.F("users", "email"), instance.P("email")).
			Set(instance.F("users", "age"), instance.P("age")).
			Set(instance.F("users", "active"), instance.P("active")).
			Build()
	}
}

// BenchmarkRangeFilter measures range filter creation.
func BenchmarkRangeFilter(b *testing.B) {
	instance := createBenchmarkInstance(b)
	field := instance.F("users", "age")
	minParam := instance.P("minAge")
	maxParam := instance.P("maxAge")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = docql.Range(field, &minParam, &maxParam)
	}
}

// BenchmarkExistsFilter measures exists filter creation.
func BenchmarkExistsFilter(b *testing.B) {
	instance := createBenchmarkInstance(b)
	field := instance.F("users", "email")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = docql.Exists(field)
	}
}

// BenchmarkRegexFilter measures regex filter creation.
func BenchmarkRegexFilter(b *testing.B) {
	instance := createBenchmarkInstance(b)
	field := instance.F("users", "username")
	pattern := instance.P("pattern")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = docql.Regex(field, pattern)
	}
}
