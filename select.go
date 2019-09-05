package sqb

import (
	"io"
	"strings"
)

type SQLWriter interface {
	io.Writer
	AddArgs(...interface{}) error
}

type SQB interface {
	WriteSQLTo(SQLWriter) error
}

type SelectStmt struct {
	cols      []string
	From      FromStmt
	WhereStmt WhereStmt
}

type FromStmt interface {
	SQB
}

//TODO: make coloumns interface (coloumn names, functions)
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

	err = s.From.WriteSQLTo(st)
	if err != nil {
		return err
	}

	// TODO: Think about unnecessary space
	_, err = st.Write([]byte(` `))
	if err != nil {
		return err
	}

	return s.WhereStmt.WriteSQLTo(st)
}

func (cs SelectStmt) Where(exprs ...BoolExpr) SelectStmt {
	cp := cs
	cp.WhereStmt = WhereStmt{
		Exprs: exprs,
	}
	return cp
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

func (InnerJoinStmt) IsJoinable()     {}
func (LeftJoinStmt) IsJoinable()      {}
func (FullOuterJoinStmt) IsJoinable() {}
func (RightJoinStmt) IsJoinable()     {}
func (CrossJoinStmt) IsJoinable()     {}
func (TableNameStmt) IsJoinable()     {}

type joinStmt struct {
	kind                  string
	LeftTable, RightTable Joinable
}

func newJoinStmt(l, r Joinable, kind string) joinStmt {
	return joinStmt{
		kind:       kind,
		LeftTable:  l,
		RightTable: r,
	}
}

func (js joinStmt) WriteSQLTo(st SQLWriter) error {
	err := js.LeftTable.WriteSQLTo(st)
	if err != nil {
		return err
	}
	_, err = st.Write([]byte(" " + js.kind + " JOIN "))
	if err != nil {
		return err
	}
	err = js.RightTable.WriteSQLTo(st)
	if err != nil {
		return err
	}
	return nil
}

type joinStmtWithOn struct {
	joinStmt
	LeftCol, RightCol string
}

func newjoinStmtWithOn(left, right Joinable, on OnStmt, kind string) joinStmtWithOn {
	return joinStmtWithOn{
		joinStmt: newJoinStmt(left, right, kind),
		LeftCol:  on.A,
		RightCol: on.B,
	}
}

func (jso joinStmtWithOn) WriteSQLTo(st SQLWriter) error {
	if err := jso.joinStmt.WriteSQLTo(st); err != nil {
		return err
	}
	_, err := st.Write([]byte(" ON " + jso.LeftCol + "=" + jso.RightCol))
	return err
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

type FullOuterJoinStmt struct {
	joinStmtWithOn
}

func FullOuterJoin(left, right Joinable, on OnStmt) FullOuterJoinStmt {
	return FullOuterJoinStmt{
		joinStmtWithOn: newjoinStmtWithOn(left, right, on, "FULL OUTER"),
	}
}

type RightJoinStmt struct {
	joinStmtWithOn
}

func RightJoin(left, right Joinable, on OnStmt) RightJoinStmt {
	return RightJoinStmt{
		joinStmtWithOn: newjoinStmtWithOn(left, right, on, "RIGHT"),
	}
}

type CrossJoinStmt struct {
	joinStmt
}

func CrossJoin(l, r Joinable) CrossJoinStmt {
	return CrossJoinStmt{
		joinStmt: newJoinStmt(l, r, "CROSS"),
	}
}

type TableNameStmt string

func TableName(n string) TableNameStmt {
	return TableNameStmt(n)
}

func (tns TableNameStmt) As(name string) TableNameAsStmt {
	return TableNameAsStmt{
		TableNameStmt: tns,
		AS:            name,
	}
}

func (tn TableNameStmt) WriteSQLTo(st SQLWriter) error {
	_, err := st.Write([]byte(tn))
	return err
}

type TableNameAsStmt struct {
	TableNameStmt
	AS string
}

func (tn TableNameAsStmt) WriteSQLTo(st SQLWriter) error {
	err := tn.TableNameStmt.WriteSQLTo(st)
	if err != nil {
		return err
	}
	_, err = st.Write([]byte(` AS ` + tn.AS))
	return err
}

type WhereStmt struct {
	Exprs []BoolExpr
}

func (ws WhereStmt) WriteSQLTo(st SQLWriter) error {
	if len(ws.Exprs) == 0 {
		return nil
	}
	_, err := st.Write([]byte(`WHERE `))
	if err != nil {
		return err
	}
	err = ws.Exprs[0].WriteSQLTo(st)
	if err != nil {
		return err
	}

	if len(ws.Exprs) == 1 {
		return nil
	}

	for _, ex := range ws.Exprs[1:] {
		_, err := st.Write([]byte(`, `))
		if err != nil {
			return err
		}
		err = ex.WriteSQLTo(st)
		if err != nil {
			return err
		}
	}
	return nil
}

type BoolExpr interface {
	SQB
}

type EqExpr struct {
	A, B Comparable
}

func Eq(a, b Comparable) EqExpr {
	return EqExpr{A: a, B: b}
}

func (ee EqExpr) WriteSQLTo(st SQLWriter) error {
	err := ee.A.WriteSQLTo(st)
	if err != nil {
		return err
	}
	_, err = st.Write([]byte(`=`))
	if err != nil {
		return err
	}
	return ee.B.WriteSQLTo(st)
}

type Comparable interface {
	IsComparable()
	SQB
}

func (Coloumn) IsComparable() {}
func (Arg) IsComparable()     {}

type Coloumn string

func (c Coloumn) WriteSQLTo(st SQLWriter) error {
	_, err := st.Write([]byte(c))
	return err
}

type Arg struct {
	V interface{}
}

func (a Arg) WriteSQLTo(st SQLWriter) error {

	if cp, ok := st.(CustomPlaceholder); ok {
		err := cp.WritePlaceholder()
		if err != nil {
			return err
		}
	} else {
		_, err := st.Write([]byte(`?`))
		if err != nil {
			return err
		}
	}

	return st.AddArgs(a.V)
}
