package sqb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJB_LeftJoin(t *testing.T) {
	var tests = [...]struct {
		name           string
		sqb            SQB
		wantErr        bool
		expectedRawSQL string
		expectedArgs   []interface{}
	}{
		{
			name:           "4 tables join",
			expectedRawSQL: `users LEFT JOIN posts ON users.id=posts.user_id RIGHT JOIN cities ON users.city_id=cities.id LEFT JOIN regions ON cities.region_id=regions.id`,
			sqb: JB(TableName("users")).
				LeftJoin(TableName("posts"), Eq(Column("users.id"), Column("posts.user_id"))).
				RightJoin(TableName("cities"), Eq(Column("users.city_id"), Column("cities.id"))).
				LeftJoin(TableName("regions"), Eq(Column("cities.region_id"), Column("regions.id"))),
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
