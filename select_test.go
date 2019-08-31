package sqb

import (
	"strings"
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
			expectedRawSQL: "SELECT * FROM users ",
			sqb:            From(TableName("users")),
		},
		{
			name:           "select from left join",
			expectedRawSQL: "SELECT * FROM users LEFT JOIN posts ON users.id=posts.user_id ",
			sqb: From(
				LeftJoin(TableName("users"), TableName("posts"), On("users.id", "posts.user_id")),
			),
		},
		{
			name:           "select ids from left join",
			expectedRawSQL: "SELECT users.id FROM users LEFT JOIN posts ON users.id=posts.user_id ",
			sqb: From(
				LeftJoin(TableName("users"), TableName("posts"), On("users.id", "posts.user_id")),
			).Select("users.id"),
		},
		{
			name:           "subquery",
			expectedRawSQL: "SELECT * FROM (SELECT * FROM users ) AS users ",
			sqb:            From(From(TableName("users")).As("users")),
		},
		{
			name:           "Where",
			expectedRawSQL: "SELECT * FROM users WHERE city=?",
			expectedArgs:   []interface{}{10},
			sqb:            From(TableName("users")).Where(Eq(Coloumn("city"), Arg{V: 10})),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqb := tt.sqb
			tsw := &testSQLWriter{}
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

type testSQLWriter struct {
	strings.Builder
	Args []interface{}
}

func (tsw *testSQLWriter) AddArgs(aa ...interface{}) error {
	tsw.Args = append(tsw.Args, aa...)
	return nil
}
