package sqb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUpdateStmt_WriteSQLTo(t *testing.T) {
	tests := []struct {
		name           string
		sqb            SQB
		wantErr        bool
		expectedRawSQL string
		expectedArgs   []interface{}
	}{
		{
			name: "simple set for all rows",
			sqb: UpdateStmt{
				Table: TableIdentifier("users"),
				Set: SetStmt{
					{
						Key:   Column("updated_at"),
						Value: Arg{V: time.Date(2020, 05, 14, 13, 32, 00, 00, time.UTC)},
					},
				},
			},
			expectedRawSQL: "UPDATE users SET updated_at = ?",
			expectedArgs:   []interface{}{time.Date(2020, 05, 14, 13, 32, 00, 00, time.UTC)},
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
