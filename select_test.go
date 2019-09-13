package sqb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	_ SQB      = FromStmt(nil)
	_ Joinable = InnerJoinStmt{}
	_ Joinable = LeftJoinStmt{}
)

func TestWriteSQLTo(t *testing.T) {
	var tests = []struct {
		name           string
		sqb            SQB
		wantErr        bool
		expectedRawSQL string
		expectedArgs   []interface{}
	}{
		{
			name:           "left join",
			expectedRawSQL: "users LEFT JOIN posts ON users.id=posts.user_id",
			sqb: LeftJoin(
				TableName("users"), TableName("posts"), On("users.id", "posts.user_id"),
			),
		},
		{
			name:           "select from users",
			expectedRawSQL: "SELECT * FROM users",
			sqb:            From(TableName("users")),
		},
		{
			name:           "select from left join",
			expectedRawSQL: "SELECT * FROM users LEFT JOIN posts ON users.id=posts.user_id",
			sqb: From(
				LeftJoin(TableName("users"), TableName("posts"), On("users.id", "posts.user_id")),
			),
		},
		{
			name:           "select ids from left join",
			expectedRawSQL: "SELECT users.id FROM users LEFT JOIN posts ON users.id=posts.user_id",
			sqb: From(
				LeftJoin(TableName("users"), TableName("posts"), On("users.id", "posts.user_id")),
			).Select(Coloumn("users.id")),
		},
		{
			name:           "subquery",
			expectedRawSQL: "SELECT * FROM (SELECT * FROM users) AS users",
			sqb:            From(From(TableName("users")).As("users")),
		},
		{
			name:           "where",
			expectedRawSQL: "SELECT * FROM users WHERE city=?",
			expectedArgs:   []interface{}{10},
			sqb:            From(TableName("users")).Where(Eq(Coloumn("city"), Arg{V: 10})),
		},
		{
			name:           "order by",
			expectedRawSQL: "SELECT * FROM users WHERE city=? ORDER BY city ASC, region DESC",
			expectedArgs:   []interface{}{10},
			sqb:            From(TableName("users")).Where(Eq(Coloumn("city"), Arg{V: 10})).OrderBy(Asc(Coloumn("city")), Desc(Coloumn("region"))),
		},
		{
			name:           "4 tables join",
			expectedRawSQL: `SELECT * FROM users LEFT JOIN posts ON users.id=posts.user_id RIGHT JOIN cities ON users.city_id=cities.id LEFT JOIN regions ON cities.region_id=regions.id`,
			sqb: From(
				LeftJoin(
					RightJoin(
						LeftJoin(TableName("users"), TableName("posts"), On("users.id", "posts.user_id")),
						TableName("cities"), On("users.city_id", "cities.id"),
					),
					TableName("regions"), On("cities.region_id", "regions.id"),
				),
			),
		},
		{
			name:           "select distinct",
			expectedRawSQL: `SELECT DISTINCT * FROM users`,
			sqb:            From(TableName("users")).Distinct(),
		},
		{
			name:           "group by",
			expectedRawSQL: `SELECT count(id) FROM users GROUP BY city_id`,
			sqb:            From(TableName("users")).Select(Count(Coloumn("id"))).GroupBy(Coloumn("city_id")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqb := tt.sqb
			tsw := &DefaultSQLWriter{}
			if err := sqb.WriteSQLTo(tsw); (err != nil) != tt.wantErr {
				t.Errorf("joinStmtWithOn.WriteSQLTo() error = %v, wantErr %v", err, tt.wantErr)
			}
			builded := tsw.String()
			if builded != tt.expectedRawSQL {
				t.Errorf("joinStmtWithOn.WriteSQLTo() raw SQL expected = %v, actual = %v", tt.expectedRawSQL, builded)
			}
			assert.Equal(t, tt.expectedArgs, tsw.Args)
		})
	}
}

func BenchmarkWriteSQLTo(t *testing.B) {
	var tests = []struct {
		name           string
		sqb            SQB
		wantErr        bool
		expectedRawSQL string
		expectedArgs   []interface{}
	}{
		{
			name:           "left join",
			expectedRawSQL: "users LEFT JOIN posts ON users.id=posts.user_id",
			sqb: LeftJoin(
				TableName("users"), TableName("posts"), On("users.id", "posts.user_id"),
			),
		},
		{
			name:           "select from users",
			expectedRawSQL: "SELECT * FROM users",
			sqb:            From(TableName("users")),
		},
		{
			name:           "select from left join",
			expectedRawSQL: "SELECT * FROM users LEFT JOIN posts ON users.id=posts.user_id",
			sqb: From(
				LeftJoin(TableName("users"), TableName("posts"), On("users.id", "posts.user_id")),
			),
		},
		{
			name:           "select ids from left join",
			expectedRawSQL: "SELECT users.id FROM users LEFT JOIN posts ON users.id=posts.user_id",
			sqb: From(
				LeftJoin(TableName("users"), TableName("posts"), On("users.id", "posts.user_id")),
			).Select(Coloumn("users.id")),
		},
		{
			name:           "subquery",
			expectedRawSQL: "SELECT * FROM (SELECT * FROM users) AS users",
			sqb:            From(From(TableName("users")).As("users")),
		},
		{
			name:           "where",
			expectedRawSQL: "SELECT * FROM users WHERE city=?",
			expectedArgs:   []interface{}{10},
			sqb:            From(TableName("users")).Where(Eq(Coloumn("city"), Arg{V: 10})),
		},
		{
			name:           "order by",
			expectedRawSQL: "SELECT * FROM users WHERE city=? ORDER BY city ASC, region DESC",
			expectedArgs:   []interface{}{10},
			sqb:            From(TableName("users")).Where(Eq(Coloumn("city"), Arg{V: 10})).OrderBy(Asc(Coloumn("city")), Desc(Coloumn("region"))),
		},
		{
			name:           "4 tables join",
			expectedRawSQL: `SELECT * FROM users LEFT JOIN posts ON users.id=posts.user_id RIGHT JOIN cities ON users.city_id=cities.id LEFT JOIN regions ON cities.region_id=regions.id`,
			sqb: From(
				LeftJoin(
					RightJoin(
						LeftJoin(TableName("users"), TableName("posts"), On("users.id", "posts.user_id")),
						TableName("cities"), On("users.city_id", "cities.id"),
					),
					TableName("regions"), On("cities.region_id", "regions.id"),
				),
			),
		},
		{
			name:           "select distinct",
			expectedRawSQL: `SELECT DISTINCT * FROM users`,
			sqb:            From(TableName("users")).Distinct(),
		},
		{
			name:           "group by",
			expectedRawSQL: `SELECT count(id) FROM users GROUP BY city_id`,
			sqb:            From(TableName("users")).Select(Count(Coloumn("id"))).GroupBy(Coloumn("city_id")),
		},
	}
	t.ResetTimer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.B) {
			for i := 0; i < t.N; i++ {
				sqb := tt.sqb
				tsw := &DefaultSQLWriter{}
				if err := sqb.WriteSQLTo(tsw); (err != nil) != tt.wantErr {
					t.Errorf("joinStmtWithOn.WriteSQLTo() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}
