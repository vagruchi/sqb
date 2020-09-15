package sqb

import (
	"strconv"
	"strings"
)

type SQLWriter interface {
	AddArgs(interface{}) error
	WriteString(string) (int, error)
	AppendRawArgs(a ...interface{}) error
}

type DefaultSQLWriter struct {
	strings.Builder
	Args []interface{}
}

func (d *DefaultSQLWriter) AddArgs(a interface{}) error {
	_, err := d.WriteString(`?`)
	if err != nil {
		return err
	}
	d.Args = append(d.Args, a)
	return nil
}

func (d *DefaultSQLWriter) AppendRawArgs(a ...interface{}) error {
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

type PostgreSQLWriter struct {
	strings.Builder
	Args []interface{}
}

func (p *PostgreSQLWriter) AddArgs(a interface{}) error {
	next := len(p.Args) + 1
	_, err := p.WriteString(`$` + strconv.Itoa(next))
	if err != nil {
		return err
	}
	p.Args = append(p.Args, a)
	return nil
}

func (p *PostgreSQLWriter) AppendRawArgs(a ...interface{}) error {
	p.Args = append(p.Args, a...)
	return nil
}

func (p *PostgreSQLWriter) WriteIdentifier(ident string) error {
	_, err := p.WriteString(`"` + ident + `"`)
	return err
}

func ToPostgreSql(s SQB) (string, []interface{}, error) {
	st := &PostgreSQLWriter{}
	err := s.WriteSQLTo(st)
	if err != nil {
		return "", nil, err
	}
	return st.Builder.String(), st.Args, nil
}
