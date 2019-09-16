package sqb

type SQB interface {
	WriteSQLTo(SQLWriter) error
}

type SelectStmt struct {
	Cols        []Col
	IsDistinct  bool
	From        Table
	WhereStmt   WhereStmt
	OrderByStmt OrderByStmt
	GroupByStmt GroupByStmt
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

type Table interface {
	SQB
	IsTable()
}

func (TableNameAsStmt) IsTable()   {}
func (InnerJoinStmt) IsTable()     {}
func (LeftJoinStmt) IsTable()      {}
func (FullOuterJoinStmt) IsTable() {}
func (RightJoinStmt) IsTable()     {}
func (CrossJoinStmt) IsTable()     {}
func (TableNameStmt) IsTable()     {}
func (SelectStmt) IsTable()        {}
func (JoinableSelect) IsTable()    {}

type Col interface {
	SQB
	IsCol()
}

func (cs SelectStmt) Select(cc ...Col) SelectStmt {
	cp := cs
	cp.Cols = cc
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

	if len(s.Cols) == 0 {
		_, err = st.WriteString("*")
		if err != nil {
			return err
		}
	} else {
		for i := 0; i < len(s.Cols)-1; i++ {
			c := s.Cols[i]
			err = c.WriteSQLTo(st)
			if err != nil {
				return err
			}
			_, err = st.WriteString(", ")
			if err != nil {
				return err
			}
		}
		err = s.Cols[len(s.Cols)-1].WriteSQLTo(st)
		if err != nil {
			return err
		}
	}
	_, err = st.WriteString(" FROM ")
	if err != nil {
		return err
	}

	err = s.From.WriteSQLTo(st)
	if err != nil {
		return err
	}

	if !s.GroupByStmt.IsEmpty() {
		_, err = st.WriteString(` `)
		if err != nil {
			return err
		}
	}

	err = s.GroupByStmt.WriteSQLTo(st)
	if err != nil {
		return err
	}

	if !s.WhereStmt.IsEmpty() {
		_, err = st.WriteString(` `)
		if err != nil {
			return err
		}
	}

	err = s.WhereStmt.WriteSQLTo(st)
	if err != nil {
		return err
	}

	if !s.OrderByStmt.IsEmpty() {
		_, err = st.WriteString(` `)
		if err != nil {
			return err
		}
	}

	return s.OrderByStmt.WriteSQLTo(st)
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
	_, err := st.WriteString(" ON " + jso.LeftCol + "=" + jso.RightCol)
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
	_, err := st.WriteString(string(tn))
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
	_, err = st.WriteString(` AS ` + tn.AS)
	return err
}

type WhereStmt struct {
	Exprs []BoolExpr
}

func (ws WhereStmt) IsEmpty() bool {
	return len(ws.Exprs) == 0
}

func (ws WhereStmt) WriteSQLTo(st SQLWriter) error {
	if len(ws.Exprs) == 0 {
		return nil
	}
	_, err := st.WriteString(`WHERE `)
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
		_, err := st.WriteString(`, `)
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
	_, err = st.WriteString(`=`)
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

func (Coloumn) IsCol() {}

func (c Coloumn) WriteSQLTo(st SQLWriter) error {
	_, err := st.WriteString(string(c))
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
		_, err := st.WriteString(`?`)
		if err != nil {
			return err
		}
	}

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

func (obs OrderByStmt) IsEmpty() bool {
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

func (gbs GroupByStmt) IsEmpty() bool {
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
