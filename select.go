package sqb

import (
	"io"
	"strings"
)

type SQLWriter interface {
	io.Writer
	AddArgs(...interface{})
}

type SQB interface {
	WriteSQLTo(SQLWriter) error
}

type SelectStmt struct {
	cols []string
	From FromStmt
}

type FromStmt interface {
	SQB
}

func (cs SelectStmt) Select(cc ...string) SelectStmt {
	cp := cs
	cp.cols = cc
	return cp
}

func From(fs FromStmt) SelectStmt {
	copycs := SelectStmt{
		From: fs,
	}
	return copycs
}

func (s SelectStmt) WriteSQLTo(st SQLWriter) error {
	rawcols := "*"

	if len(s.cols) > 0 {
		rawcols = strings.Join(s.cols, ", ")
	}

	_, err := st.Write([]byte(`SELECT ` + rawcols + ` FROM `))
	if err != nil {
		return err
	}

	return s.From.WriteSQLTo(st)
}

func (s SelectStmt) As(name string) JoinableSelect {
	return JoinableSelect{
		SelectStmt: s,
		AS:         name,
	}
}

type JoinableSelect struct {
	SelectStmt
	AS string
}

func (js JoinableSelect) WriteSQLTo(st SQLWriter) error {
	if _, err := st.Write([]byte(`(`)); err != nil {
		return err
	}
	if err := js.SelectStmt.WriteSQLTo(st); err != nil {
		return err
	}
	_, err := st.Write([]byte(`) AS ` + js.AS))
	return err
}

type Joinable interface {
	SQB
	IsJoinable()
}

func (InnerJoinStmt) IsJoinable() {}
func (LeftJoinStmt) IsJoinable()  {}
func (TableNameStmt) IsJoinable() {}

type joinStmtWithOn struct {
	kind                  string
	LeftTable, RightTable FromStmt
	LeftCol, RightCol     string
}

func newjoinStmtWithOn(Left, Right Joinable, on OnStmt, kind string) joinStmtWithOn {
	return joinStmtWithOn{
		LeftTable:  Left,
		RightTable: Right,
		LeftCol:    on.A,
		RightCol:   on.B,
		kind:       kind,
	}
}

type InnerJoinStmt struct {
	joinStmtWithOn
}

type OnStmt struct {
	A, B string
}

func On(a, b string) OnStmt {
	return OnStmt{A: a, B: b}
}

func InnerJoin(left, right Joinable, on OnStmt) InnerJoinStmt {
	return InnerJoinStmt{
		joinStmtWithOn: newjoinStmtWithOn(left, right, on, "INNER"),
	}
}

type LeftJoinStmt struct {
	joinStmtWithOn
}

func LeftJoin(left, right Joinable, on OnStmt) LeftJoinStmt {
	return LeftJoinStmt{
		joinStmtWithOn: newjoinStmtWithOn(left, right, on, "LEFT"),
	}
}

func (jso joinStmtWithOn) WriteSQLTo(st SQLWriter) error {
	err := jso.LeftTable.WriteSQLTo(st)
	if err != nil {
		return err
	}
	_, err = st.Write([]byte(" " + jso.kind + " JOIN "))
	if err != nil {
		return err
	}
	err = jso.RightTable.WriteSQLTo(st)
	if err != nil {
		return err
	}

	_, err = st.Write([]byte(" ON " + jso.LeftCol + "=" + jso.RightCol))
	if err != nil {
		return err
	}
	return nil
}

type TableNameStmt string

func TableName(n string) TableNameStmt {
	return TableNameStmt(n)
}

func (tn TableNameStmt) WriteSQLTo(st SQLWriter) error {
	_, err := st.Write([]byte(tn))
	return err
}
