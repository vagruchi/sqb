package sqb

type Identifier string

func (i Identifier) WriteSQLTo(w SQLWriter) error {
	if wi, ok := w.(IdentifierWriter); ok {
		return wi.WriteIdentifier(string(i))
	}
	_, err := w.WriteString(string(i))
	return err
}

func (Identifier) As()    {}
func (Identifier) IsCol() {}

type PrefixIdentifier struct {
	Prefix, Ident Identifier
}

func (pi PrefixIdentifier) WriteSQLTo(w SQLWriter) error {
	err := pi.Prefix.WriteSQLTo(w)
	if err != nil {
		return err
	}

	_, err = w.WriteString(".")
	if err != nil {
		return err
	}

	err = pi.Ident.WriteSQLTo(w)
	return err
}

func (PrefixIdentifier) IsCol() {}
func (PrefixIdentifier) As()    {}

type AllowedAs interface {
	SQB
	As()
}

type NamedIdentifier struct {
	Original AllowedAs
	New      Identifier
}

func (NamedIdentifier) IsCol()   {}
func (NamedIdentifier) IsTable() {}

func (ni NamedIdentifier) WriteSQLTo(w SQLWriter) error {
	err := ni.Original.WriteSQLTo(w)
	if err != nil {
		return err
	}

	_, err = w.WriteString(" AS ")
	if err != nil {
		return err
	}

	err = ni.New.WriteSQLTo(w)
	if err != nil {
		return err
	}
	return nil
}
