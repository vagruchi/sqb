package sqb

import (
	"strconv"
)

type SQB interface {
	WriteSQLTo(SQLWriter) error
}

type ColumnListI interface {
	SQB
	IsColumnList()
}

type ColumnList struct {
	Cols   []Col
	Prefix string
}

func (ColumnList) IsColumnList() {}

func NewColumnList(cols ...Col) ColumnList {
	return ColumnList{
		Cols: cols,
	}
}

func (cl ColumnList) WithPrefix(prefix string) ColumnList {
	cl.Prefix = prefix
	return cl
}

type ReturningStmt struct {
	Cols ColumnListI
}

func (cl ReturningStmt) WriteSQLTo(w SQLWriter) error {
	_, err := w.WriteString(" RETURNING ")
	if err != nil {
		return err
	}

	return cl.Cols.WriteSQLTo(w)
}

func (cl ColumnList) WriteSQLTo(w SQLWriter) error {
	var err error
	if len(cl.Cols) == 0 {
		_, err = w.WriteString("*")
		return err
	}

	if cl.Prefix != "" {
		_, err = w.WriteString(cl.Prefix + ".")
		if err != nil {
			return err
		}
	}

	err = cl.Cols[0].WriteSQLTo(w)
	if err != nil {
		return err
	}

	if len(cl.Cols) == 1 {
		return nil
	}

	for _, c := range cl.Cols[1:] {
		_, err = w.WriteString(", ")
		if err != nil {
			return err
		}

		if cl.Prefix != "" {
			_, err = w.WriteString(cl.Prefix + ".")
			if err != nil {
				return err
			}
		}

		err = c.WriteSQLTo(w)
		if err != nil {
			return err
		}
	}

	return nil
}

type SelectStmt struct {
	Cols        ColumnListI
	IsDistinct  bool
	IsForUpdate bool
	From        Table
	WhereStmt   WhereStmt
	OrderByStmt OrderByStmt
	GroupByStmt GroupByStmt
	LimitStmt   LimitStmt
	OffsetStmt  OffsetStmt
}

func (cs SelectStmt) ForUpdate() SelectStmt {
	cp := cs
	cp.IsForUpdate = true
	return cp
}

func (cs SelectStmt) Distinct() SelectStmt {
	cp := cs
	cp.IsDistinct = true
	return cp
}

func (cs SelectStmt) OrderBy(ob ...OrderByElem) SelectStmt {
	cp := cs
	cp.OrderByStmt.Elems = ob
	return cp
}

func (cs SelectStmt) GroupBy(cc ...Col) SelectStmt {
	cp := cs
	cp.GroupByStmt.Cols = cc
	return cp
}

func (cs SelectStmt) SelectList(cl ColumnListI) SelectStmt {
	cs.Cols = cl
	return cs
}

type Table interface {
	SQB
	IsTable()
}

func (TableIdentifierAlias) IsTable() {}
func (InnerJoinStmt) IsTable()        {}
func (LeftJoinStmt) IsTable()         {}
func (FullOuterJoinStmt) IsTable()    {}
func (RightJoinStmt) IsTable()        {}
func (CrossJoinStmt) IsTable()        {}
func (TableIdentifier) IsTable()      {}
func (SelectStmt) IsTable()           {}
func (SubqueryAlias) IsTable()        {}

type Col interface {
	SQB
	IsCol()
}

func (cs SelectStmt) Select(cc ...Col) SelectStmt {
	cp := cs
	cp.Cols = NewColumnList(cc...)
	return cp
}

func From(fs Table) SelectStmt {
	copycs := SelectStmt{
		From: fs,
	}
	return copycs
}

func (s SelectStmt) WriteSQLTo(st SQLWriter) error {
	_, err := st.WriteString(`SELECT `)
	if err != nil {
		return err
	}

	if s.IsDistinct {
		_, err := st.WriteString(`DISTINCT `)
		if err != nil {
			return err
		}
	}

	if s.Cols == nil {
		s.Cols = NewColumnList()
	}

	err = s.Cols.WriteSQLTo(st)
	if err != nil {
		return err
	}

	_, err = st.WriteString(" FROM ")
	if err != nil {
		return err
	}

	err = s.From.WriteSQLTo(st)
	if err != nil {
		return err
	}

	if !s.WhereStmt.Empty() {
		_, err = st.WriteString(` `)
		if err != nil {
			return err
		}

		err = s.WhereStmt.WriteSQLTo(st)
		if err != nil {
			return err
		}
	}

	if !s.GroupByStmt.Empty() {
		_, err = st.WriteString(` `)
		if err != nil {
			return err
		}

		err = s.GroupByStmt.WriteSQLTo(st)
		if err != nil {
			return err
		}
	}

	if !s.OrderByStmt.Empty() {
		_, err = st.WriteString(` `)
		if err != nil {
			return err
		}

		err = s.OrderByStmt.WriteSQLTo(st)
		if err != nil {
			return err
		}
	}

	if !s.LimitStmt.Empty() {
		_, err = st.WriteString(` `)
		if err != nil {
			return err
		}

		err = s.LimitStmt.WriteSQLTo(st)
		if err != nil {
			return err
		}
	}

	if !s.OffsetStmt.Empty() {
		_, err = st.WriteString(` `)
		if err != nil {
			return err
		}

		err = s.OffsetStmt.WriteSQLTo(st)
		if err != nil {
			return err
		}
	}
	// must be last statement in query
	if s.IsForUpdate {
		_, err := st.WriteString(` FOR UPDATE`)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs SelectStmt) Where(exprs ...BoolExpr) SelectStmt {
	cp := cs
	cp.WhereStmt = WhereStmt{
		Exprs: exprs,
	}
	return cp
}

func (cs SelectStmt) Limit(limit uint64) SelectStmt {
	cp := cs
	cp.LimitStmt = LimitStmt{
		V: limit,
	}
	return cp
}

// TODO: allow it only for limited query
func (cs SelectStmt) Offset(offset uint64) SelectStmt {
	cp := cs
	cp.OffsetStmt = OffsetStmt{
		V: offset,
	}
	return cp
}

func (s SelectStmt) As(name string) SubqueryAlias {
	return SubqueryAlias{
		SelectStmt: s,
		AS:         name,
	}
}

type SubqueryAlias struct {
	SelectStmt
	AS string
}

func (js SubqueryAlias) WriteSQLTo(st SQLWriter) error {
	if _, err := st.WriteString(`(`); err != nil {
		return err
	}
	if err := js.SelectStmt.WriteSQLTo(st); err != nil {
		return err
	}
	_, err := st.WriteString(`) AS ` + js.AS)
	return err
}

type Joinable interface {
	Table
	IsJoinable()
}

func (InnerJoinStmt) IsJoinable()     {}
func (LeftJoinStmt) IsJoinable()      {}
func (FullOuterJoinStmt) IsJoinable() {}
func (RightJoinStmt) IsJoinable()     {}
func (CrossJoinStmt) IsJoinable()     {}
func (TableIdentifier) IsJoinable()   {}
func (SubqueryAlias) IsJoinable()     {}

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
	_, err = st.WriteString(" " + js.kind + " JOIN ")
	if err != nil {
		return err
	}
	err = js.RightTable.WriteSQLTo(st)
	if err != nil {
		return err
	}
	return nil
}

type OnExpr interface {
	SQB
	IsOnExpr()
}

func (EqExpr) IsOnExpr()    {}
func (OnAndExpr) IsOnExpr() {}
func (OnOrExpr) IsOnExpr()  {}
func (OnInExpr) IsOnExpr()  {}

type joinStmtWithOn struct {
	joinStmt
	on OnExpr
}

type OnAndExpr struct {
	Exprs []OnExpr
}

func (oe OnAndExpr) WriteSQLTo(st SQLWriter) error {
	if len(oe.Exprs) == 0 {
		return nil
	}
	_, err := st.WriteString(`(`)
	if err != nil {
		return err
	}

	err = oe.Exprs[0].WriteSQLTo(st)
	if err != nil {
		return err
	}

	_, err = st.WriteString(`)`)
	if err != nil {
		return err
	}

	if len(oe.Exprs) == 1 {
		return nil
	}

	for _, ex := range oe.Exprs[1:] {
		_, err := st.WriteString(` AND `)
		if err != nil {
			return err
		}
		_, err = st.WriteString(`(`)
		if err != nil {
			return err
		}

		err = ex.WriteSQLTo(st)
		if err != nil {
			return err
		}
		_, err = st.WriteString(`)`)
		if err != nil {
			return err
		}

	}
	return nil
}

type OnOrExpr struct {
	Exprs []OnExpr
}

func (oe OnOrExpr) WriteSQLTo(st SQLWriter) error {
	if len(oe.Exprs) == 0 {
		return nil
	}
	_, err := st.WriteString(`(`)
	if err != nil {
		return err
	}

	err = oe.Exprs[0].WriteSQLTo(st)
	if err != nil {
		return err
	}

	_, err = st.WriteString(`)`)
	if err != nil {
		return err
	}

	if len(oe.Exprs) == 1 {
		return nil
	}

	for _, ex := range oe.Exprs[1:] {
		_, err := st.WriteString(` OR `)
		if err != nil {
			return err
		}
		_, err = st.WriteString(`(`)
		if err != nil {
			return err
		}

		err = ex.WriteSQLTo(st)
		if err != nil {
			return err
		}
		_, err = st.WriteString(`)`)
		if err != nil {
			return err
		}

	}
	return nil
}

type OnInExpr struct {
	// improve name
	Some Comparable
	In   []Comparable
}

func (oe OnInExpr) WriteSQLTo(st SQLWriter) error {
	err := oe.Some.WriteSQLTo(st)
	if err != nil {
		return err
	}

	_, err = st.WriteString(` IN `)
	if err != nil {
		return err
	}

	for _, ex := range oe.In {
		err = ex.WriteSQLTo(st)
		if err != nil {
			return err
		}
	}
	return nil
}

func newjoinStmtWithOn(left, right Joinable, on OnExpr, kind string) joinStmtWithOn {
	return joinStmtWithOn{
		joinStmt: newJoinStmt(left, right, kind),
		on:       on,
	}
}

func (jso joinStmtWithOn) WriteSQLTo(st SQLWriter) error {
	if err := jso.joinStmt.WriteSQLTo(st); err != nil {
		return err
	}
	if _, err := st.WriteString(" ON "); err != nil {
		return err
	}
	return jso.on.WriteSQLTo(st)
}

type InnerJoinStmt struct {
	joinStmtWithOn
}

func InnerJoin(left, right Joinable, on OnExpr) InnerJoinStmt {
	return InnerJoinStmt{
		joinStmtWithOn: newjoinStmtWithOn(left, right, on, "INNER"),
	}
}

type LeftJoinStmt struct {
	joinStmtWithOn
}

func LeftJoin(left, right Joinable, on OnExpr) LeftJoinStmt {
	return LeftJoinStmt{
		joinStmtWithOn: newjoinStmtWithOn(left, right, on, "LEFT"),
	}
}

type FullOuterJoinStmt struct {
	joinStmtWithOn
}

func FullOuterJoin(left, right Joinable, on OnExpr) FullOuterJoinStmt {
	return FullOuterJoinStmt{
		joinStmtWithOn: newjoinStmtWithOn(left, right, on, "FULL OUTER"),
	}
}

type RightJoinStmt struct {
	joinStmtWithOn
}

func RightJoin(left, right Joinable, on OnExpr) RightJoinStmt {
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

type TableIdentifier string

func TableName(n string) TableIdentifier {
	return TableIdentifier(n)
}

func (tns TableIdentifier) As(name string) TableIdentifierAlias {
	return TableIdentifierAlias{
		TableIdentifier: tns,
		AS:              name,
	}
}

func (tn TableIdentifier) WriteSQLTo(st SQLWriter) error {
	_, err := st.WriteString(string(tn))
	return err
}

type TableIdentifierAlias struct {
	TableIdentifier
	AS string
}

func (tn TableIdentifierAlias) WriteSQLTo(st SQLWriter) error {
	err := tn.TableIdentifier.WriteSQLTo(st)
	if err != nil {
		return err
	}
	_, err = st.WriteString(` AS ` + tn.AS)
	return err
}

type WhereStmt struct {
	Exprs []BoolExpr
}

func (ws WhereStmt) Empty() bool {
	return len(ws.Exprs) == 0
}

func (ws WhereStmt) WriteSQLTo(st SQLWriter) error {
	if len(ws.Exprs) == 0 {
		return nil
	}
	_, err := st.WriteString(`WHERE (`)
	if err != nil {
		return err
	}
	err = ws.Exprs[0].WriteSQLTo(st)
	if err != nil {
		return err
	}
	_, err = st.WriteString(`)`)
	if err != nil {
		return err
	}

	if len(ws.Exprs) == 1 {
		return nil
	}

	for _, ex := range ws.Exprs[1:] {
		_, err := st.WriteString(` AND `)
		if err != nil {
			return err
		}
		_, err = st.WriteString(`(`)
		if err != nil {
			return err
		}

		err = ex.WriteSQLTo(st)
		if err != nil {
			return err
		}
		_, err = st.WriteString(`)`)
		if err != nil {
			return err
		}

	}
	return nil
}

type Column string

func (Column) IsCol() {}

func (c Column) WriteSQLTo(st SQLWriter) error {
	_, err := st.WriteString(string(c))
	return err
}

type Arg struct {
	V interface{}
}

func (a Arg) IsCol() {}

func (a Arg) WriteSQLTo(st SQLWriter) error {
	return st.AddArgs(a.V)
}

type OrderKind string

const (
	AscOrder  OrderKind = "ASC"
	DescOrder OrderKind = "DESC"
)

type OrderByElem struct {
	C    Col
	Kind OrderKind
}

func (obe OrderByElem) WriteSQLTo(st SQLWriter) error {
	err := obe.C.WriteSQLTo(st)
	if err != nil {
		return err
	}
	_, err = st.WriteString(" " + string(obe.Kind))
	return err
}

type OrderByStmt struct {
	Elems []OrderByElem
}

func (obs OrderByStmt) Empty() bool {
	return len(obs.Elems) == 0
}

func (obs OrderByStmt) WriteSQLTo(st SQLWriter) error {
	if len(obs.Elems) == 0 {
		return nil
	}
	_, err := st.WriteString("ORDER BY ")
	if err != nil {
		return err
	}

	err = obs.Elems[0].WriteSQLTo(st)
	if err != nil {
		return err
	}

	if len(obs.Elems) == 1 {
		return nil
	}

	for _, el := range obs.Elems[1:] {
		_, err = st.WriteString(", ")
		if err != nil {
			return err
		}
		err = el.WriteSQLTo(st)
		if err != nil {
			return err
		}
	}
	return nil
}

func Asc(c Col) OrderByElem {
	return OrderByElem{
		C:    c,
		Kind: AscOrder,
	}
}

func Desc(c Col) OrderByElem {
	return OrderByElem{
		C:    c,
		Kind: DescOrder,
	}
}

type GroupByStmt struct {
	Cols []Col
}

func (gbs GroupByStmt) Empty() bool {
	return len(gbs.Cols) == 0
}

func (gbs GroupByStmt) WriteSQLTo(st SQLWriter) error {
	if len(gbs.Cols) == 0 {
		return nil
	}
	_, err := st.WriteString("GROUP BY ")
	if err != nil {
		return err
	}

	err = gbs.Cols[0].WriteSQLTo(st)
	if err != nil {
		return err
	}

	if len(gbs.Cols) == 1 {
		return nil
	}

	for _, el := range gbs.Cols[1:] {
		_, err = st.WriteString(", ")
		if err != nil {
			return err
		}
		err = el.WriteSQLTo(st)
		if err != nil {
			return err
		}
	}
	return nil
}

type AggrFuncCall struct {
	Name       string
	Args       []SQB
	IsDistinct bool
}

func (afc AggrFuncCall) Distinct() AggrFuncCall {
	nafc := afc
	nafc.IsDistinct = true
	return nafc
}

func (AggrFuncCall) IsCol() {}

func (fc AggrFuncCall) WriteSQLTo(st SQLWriter) error {
	_, err := st.WriteString(fc.Name + "(")
	if err != nil {
		return err
	}

	if fc.IsDistinct {
		_, err := st.WriteString(`DISTINCT `)
		if err != nil {
			return err
		}
	}

	err = fc.writeArgs(st)
	if err != nil {
		return err
	}

	_, err = st.WriteString(")")
	if err != nil {
		return err
	}
	return nil
}

func (fc AggrFuncCall) writeArgs(st SQLWriter) error {
	if len(fc.Args) == 0 {
		return nil
	}
	err := fc.Args[0].WriteSQLTo(st)
	if err != nil {
		return err
	}

	if len(fc.Args) == 1 {
		return nil
	}

	for _, el := range fc.Args[1:] {
		_, err = st.WriteString(", ")
		if err != nil {
			return err
		}
		err = el.WriteSQLTo(st)
		if err != nil {
			return err
		}
	}
	return nil
}

func Count(args ...Col) AggrFuncCall {
	a := make([]SQB, 0, len(args))
	for _, e := range args {
		a = append(a, e)
	}
	return AggrFuncCall{
		Name: "COUNT",
		Args: a,
	}
}

func Max(args ...Col) AggrFuncCall {
	a := make([]SQB, 0, len(args))
	for _, e := range args {
		a = append(a, e)
	}
	return AggrFuncCall{
		Name: "MAX",
		Args: a,
	}
}

func Min(args ...Col) AggrFuncCall {
	a := make([]SQB, 0, len(args))
	for _, e := range args {
		a = append(a, e)
	}
	return AggrFuncCall{
		Name: "MIN",
		Args: a,
	}
}

func Sum(args ...Col) AggrFuncCall {
	a := make([]SQB, 0, len(args))
	for _, e := range args {
		a = append(a, e)
	}
	return AggrFuncCall{
		Name: "SUM",
		Args: a,
	}
}

func Avg(args ...Col) AggrFuncCall {
	a := make([]SQB, 0, len(args))
	for _, e := range args {
		a = append(a, e)
	}
	return AggrFuncCall{
		Name: "AVG",
		Args: a,
	}
}

const (
	OffsetKeyword = Keyword("OFFSET")
	LimitKeyword  = Keyword("LIMIT")
)

type Keyword string

func (k Keyword) WriteSQLTo(st SQLWriter) error {
	_, err := st.WriteString(string(k))
	return err
}

type OffsetStmt struct {
	V uint64
}

func (os OffsetStmt) Empty() bool {
	return os.V == 0
}

func (os OffsetStmt) WriteSQLTo(st SQLWriter) error {
	err := OffsetKeyword.WriteSQLTo(st)
	if err != nil {
		return err
	}
	_, err = st.WriteString(" " + strconv.FormatUint(os.V, 10))
	return err
}

type LimitStmt struct {
	V uint64
}

func (ls LimitStmt) Empty() bool {
	return ls.V == 0
}

func (ls LimitStmt) WriteSQLTo(st SQLWriter) error {
	err := LimitKeyword.WriteSQLTo(st)
	if err != nil {
		return err
	}
	_, err = st.WriteString(" " + strconv.FormatUint(ls.V, 10))
	return err
}
