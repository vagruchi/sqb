package sqb

import (
	"io"
	"strings"
)

type SQLWriter interface {
	io.Writer
	AddArgs(...interface{}) error
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
