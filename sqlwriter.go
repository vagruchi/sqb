package sqb

import (
	"strings"
)

type SQLWriter interface {
	AddArgs(...interface{}) error
	WriteString(string) (int, error)
}

type DefaultSQLWriter struct {
	strings.Builder
	Args []interface{}
}

func (d *DefaultSQLWriter) AddArgs(a ...interface{}) error {
	d.Args = append(d.Args, a...)
	return nil
}

func ToSQL(s SQB) (string, []interface{}, error) {
	st := &DefaultSQLWriter{}
	err := s.WriteSQLTo(st)
	if err != nil {
		return "", nil, err
	}
	return st.Builder.String(), st.Args, nil
}
