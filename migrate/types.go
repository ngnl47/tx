package migrate

type Report struct {
	MigratedTypes int
	LegacyAliases []string
	UnknownParams []Diagnostic
	MissingFields []Diagnostic
}

type Diagnostic struct {
	Type     string
	Field    string
	Property string
	Message  string
}
