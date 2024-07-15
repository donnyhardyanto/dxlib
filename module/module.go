package module

type DXModuleInterface interface {
	DefineConfiguration() (err error)
	DefineAPI() (err error)
	Start() (err error)
	Stop() (err error)
}

type DXModule struct {
	DXModuleInterface
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