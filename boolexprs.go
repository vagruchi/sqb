package sqb

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

type OrExpr struct {
	Exprs []BoolExpr
}

func Or(expr ...BoolExpr) OrExpr {
	return OrExpr{
		Exprs: expr,
	}
}

func (oe OrExpr) WriteSQLTo(st SQLWriter) error {
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

type AndExpr struct {
	Exprs []BoolExpr
}

func And(expr ...BoolExpr) AndExpr {
	return AndExpr{
		Exprs: expr,
	}
}

func (ae AndExpr) WriteSQLTo(st SQLWriter) error {
	if len(ae.Exprs) == 0 {
		return nil
	}
	_, err := st.WriteString(`(`)
	if err != nil {
		return err
	}

	err = ae.Exprs[0].WriteSQLTo(st)
	if err != nil {
		return err
	}

	_, err = st.WriteString(`)`)
	if err != nil {
		return err
	}

	if len(ae.Exprs) == 1 {
		return nil
	}

	for _, ex := range ae.Exprs[1:] {
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

type Comparable interface {
	IsComparable()
	SQB
}

func (Column) IsComparable() {}
func (Arg) IsComparable()    {}

type NullCheck struct {
	A      Comparable
	IsNull bool
}

func (nc NullCheck) WriteSQLTo(w SQLWriter) error {
	err := nc.A.WriteSQLTo(w)
	if err != nil {
		return err
	}

	str := ` IS `
	if !nc.IsNull {
		str += `NOT `
	}

	str += `NULL`

	_, err = w.WriteString(str)
	return err
}
