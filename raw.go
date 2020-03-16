package sqb

type RawSQL struct {
	Query string
	Args  []interface{}
}

func (rsql RawSQL) WriteSQLTo(st SQLWriter) error {
	_, err := st.WriteString(rsql.Query)
	if err != nil {
		return err
	}
	return st.AppendRawArgs(rsql.Args...)
}

func (RawSQL) IsJoinable()   {}
func (RawSQL) IsComparable() {}
func (RawSQL) IsTable()      {}
func (RawSQL) IsCol()        {}
func (RawSQL) IsOnExpr()     {}

func Raw(query string, args ...interface{}) RawSQL {
	return RawSQL{
		Query: query,
		Args:  args,
	}
}
