package sqb

type SetArg struct {
	Key   Column
	Value Col
}

func (sa SetArg) WriteSQLTo(w SQLWriter) error {
	err := sa.Key.WriteSQLTo(w)
	if err != nil {
		return err
	}

	_, err = w.WriteString(` = `)
	if err != nil {
		return err
	}

	return sa.Value.WriteSQLTo(w)
}

type SetStmt []SetArg

func (ss SetStmt) WriteSQLTo(w SQLWriter) error {
	if len(ss) == 0 {
		return nil
	}

	_, err := w.WriteString(`SET `)
	if err != nil {
		return err
	}

	err = ss[0].WriteSQLTo(w)
	if err != nil {
		return err
	}

	if len(ss) == 1 {
		return nil
	}

	for _, s := range ss[1:] {
		_, err = w.WriteString(", ")
		if err != nil {
			return err
		}

		err = s.WriteSQLTo(w)
		if err != nil {
			return err
		}
	}

	return nil
}

type UpdateStmt struct {
	Table      TableIdentifier
	Set        SetStmt
	WhereStmt  WhereStmt
	ReturnCols ReturnCols
}

func (us UpdateStmt) WriteSQLTo(w SQLWriter) error {
	_, err := w.WriteString(`UPDATE `)
	if err != nil {
		return err
	}

	err = us.Table.WriteSQLTo(w)
	if err != nil {
		return err
	}

	_, err = w.WriteString(` `)
	if err != nil {
		return err
	}

	err = us.Set.WriteSQLTo(w)
	if err != nil {
		return err
	}

	if !us.WhereStmt.Empty() {
		_, err = w.WriteString(` `)
		if err != nil {
			return err
		}

		err = us.WhereStmt.WriteSQLTo(w)
		if err != nil {
			return err
		}
	}
	// must be last statement
	err = us.ReturnCols.WriteSQLTo(w)
	if err != nil {
		return err
	}

	return nil
}

type ReturnCols ColumnList

func (us UpdateStmt) Returning(cc ...Col) UpdateStmt {
	us.ReturnCols = ReturnCols(NewColumnList(cc...))
	return us
}

func (rc ReturnCols) WriteSQLTo(w SQLWriter) error {
	if len(rc.Cols) > 0 {
		_, err := w.WriteString(" RETURNING ")
		if err != nil {
			return err
		}

		err = rc.Cols[0].WriteSQLTo(w)
		if err != nil {
			return err
		}

		if len(rc.Cols) == 1 {
			return nil
		}

		for _, c := range rc.Cols[1:] {
			_, err = w.WriteString(", ")
			if err != nil {
				return err
			}
			err = c.WriteSQLTo(w)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
