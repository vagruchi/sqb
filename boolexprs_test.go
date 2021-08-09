package sqb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NotExpr(t *testing.T) {
	var tests = []struct {
		name           string
		sqb            SQB
		wantErr        bool
		expectedRawSQL string
		expectedArgs   []interface{}
	}{
		{
			name:           "subquery",
			expectedRawSQL: "SELECT * FROM users AS users WHERE (NOT (age=10))",
			sqb:            From(TableName("users").As("users")).Where(Not(Eq(Column("age"), Column("10")))),
		},
		{
			name:           "subquery",
			expectedRawSQL: "SELECT * FROM users AS users WHERE (NOT ((age=10) AND (age=99)))",
			sqb:            From(TableName("users").As("users")).Where(Not(And(Eq(Column("age"), Column("10")), Eq(Column("age"), Column("99"))))),
		},
		{
			name:           "subquery",
			expectedRawSQL: "SELECT * FROM users AS users WHERE (NOT ((age=10) OR (age=99)))",
			sqb:            From(TableName("users").As("users")).Where(Not(Or(Eq(Column("age"), Column("10")), Eq(Column("age"), Column("99"))))),
		},
		{
			name:           "subquery",
			expectedRawSQL: "SELECT * FROM users AS users WHERE (NOT (exists(SELECT * FROM statuses WHERE (statuses.active=?) AND (users.id=statuses.user_id))))",
			expectedArgs:   []interface{}{true},
			sqb: From(TableName("users").As("users")).
				Where(Not(ExistsStmt{
					Select: From(TableName("statuses")).Where(Eq(Column("statuses.active"), Arg{V: true}), Eq(Column("users.id"), Column("statuses.user_id")))})),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqb := tt.sqb
			tsw := &DefaultSQLWriter{}
			if err := sqb.WriteSQLTo(tsw); (err != nil) != tt.wantErr {
				t.Errorf("WriteSQLTo() error = %v, wantErr %v", err, tt.wantErr)
			}
			builded := tsw.String()
			if builded != tt.expectedRawSQL {
				t.Errorf("WriteSQLTo() raw SQL expected = %v, actual = %v", tt.expectedRawSQL, builded)
			}
			assert.Equal(t, tt.expectedArgs, tsw.Args)
		})
	}
}
