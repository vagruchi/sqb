package sqb

type InsertStmt struct {
	Table   TableIdentifier
	Columns []Column
	Source  InsertSource
}

type InsertSource interface {
	SQB
	IsInsertSource()
}

func (ivs InsertValuesStmt) IsInsertSource() {}
func (ss SelectStmt) IsInsertSource()        {}

func Insert(table TableIdentifier, columns []Column, source InsertSource) InsertStmt {
	return InsertStmt{
		Table:   table,
		Columns: columns,
		Source:  source,
	}
}

func (is InsertStmt) WriteSQLTo(w SQLWriter) error {
	_, err := w.WriteString(`INSERT INTO `)
	if err != nil {
		return err
	}

	err = is.Table.WriteSQLTo(w)
	if err != nil {
		return err
	}

	if len(is.Columns) > 0 {
		_, err = w.WriteString("(")
		if err != nil {
			return err
		}

		err = is.Columns[0].WriteSQLTo(w)
		if err != nil {
			return err
		}

		if len(is.Columns) > 1 {
			for _, c := range is.Columns[1:] {
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

		_, err = w.WriteString(") ")
		if err != nil {
			return err
		}
	}

	return is.Source.WriteSQLTo(w)
}

type InsertValue interface {
	SQB
	IsInsertValue()
}

func (arg Arg) IsInsertValue()        {}
func (dw defaultWord) IsInsertValue() {}

type InsertValuesStmt [][]InsertValue

func (ivs InsertValuesStmt) WriteSQLTo(w SQLWriter) error {
	if len(ivs) == 0 {
		_, err := w.WriteString("DEFAULT VALUES")
		if err != nil {
			return err
		}

		return nil
	}

	_, err := w.WriteString("VALUES ")
	if err != nil {
		return err
	}

	err = writeLine(w, ivs[0])
	if err != nil {
		return err
	}

	if len(ivs) == 1 {
		return nil
	}

	for _, values := range ivs[1:] {
		_, err = w.WriteString(", ")
		if err != nil {
			return err
		}

		err = writeLine(w, values)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeLine(w SQLWriter, values []InsertValue) error {
	if len(values) == 0 {
		return nil
	}

	_, err := w.WriteString("(")
	if err != nil {
		return err
	}

	err = values[0].WriteSQLTo(w)
	if err != nil {
		return err
	}

	if len(values) > 1 {
		for _, val := range values[1:] {
			_, err = w.WriteString(", ")
			if err != nil {
				return err
			}

			err = val.WriteSQLTo(w)
			if err != nil {
				return err
			}
		}
	}

	_, err = w.WriteString(")")
	if err != nil {
		return err
	}

	return nil
}

var Default InsertValue = defaultWord{}

type defaultWord struct{}

func (dw defaultWord) WriteSQLTo(w SQLWriter) error {
	_, err := w.WriteString("DEFAULT")
	return err
}
