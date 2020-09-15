package sqb

type CustomPlaceholder interface {
	WritePlaceholder() error
}

type IdentifierWriter interface {
	WriteIdentifier(string) error
}
