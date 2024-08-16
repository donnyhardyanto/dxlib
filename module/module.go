package module

type DXModuleInterface interface {
	DefineConfiguration() (err error)
	DefineAPI() (err error)
	Start() (err error)
	Stop() (err error)
	DoAfterConfigurationStartAll() (err error)
}

type DXInitModuleInterface interface {
	DXModuleInterface
	RegisterPrefixedKeyword()
	ResolvePrefixKeyword(text string) (err error)
}
type DXModule struct {
	DXModuleInterface
	NameId string
}

type DXInitModule struct {
	DXInitModuleInterface
	NameId        string
	PrefixKeyword string
}

func (m *DXModule) DefineConfiguration() (err error) {
	return nil
}

func (m *DXModule) DefineAPI() (err error) {
	return nil
}

func (m *DXModule) Start() (err error) {
	return nil
}

func (m *DXModule) Stop() (err error) {
	return nil
}

func (m *DXModule) DoAfterConfigurationStartAll() (err error) {
	return nil
}

func (im *DXInitModule) ResolvePrefixKeyword(text string) (err error) {
	return nil
}
func (m *DXInitModule) DefineConfiguration() (err error) {
	return nil
}

func (m *DXInitModule) DefineAPI() (err error) {
	return nil
}

func (m *DXInitModule) Start() (err error) {
	return nil
}

func (m *DXInitModule) Stop() (err error) {
	return nil
}

func (m *DXInitModule) DoAfterConfigurationStartAll() (err error) {
	return nil
}
