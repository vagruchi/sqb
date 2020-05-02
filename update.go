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
	Table TableIdentifier
	Set   SetStmt
	Where WhereStmt
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

	_, err = w.WriteString(` `)
	if err != nil {
		return err
	}

	err = us.Where.WriteSQLTo(w)
	return err
}
