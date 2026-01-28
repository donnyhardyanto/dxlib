package module

type DXModuleInterface interface {
}

type DXModule struct {
	DXModuleInterface
	NameId                 string
	DatabaseNameId         string
	ReadOnlyDatabaseNameId string
}
