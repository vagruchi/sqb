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
	return st.AddArgs(rsql.Args...)
}

func (RawSQL) IsJoinable()   {}
func (RawSQL) IsComparable() {}
