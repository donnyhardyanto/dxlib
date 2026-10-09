package app

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
)

// OpenAPIDumpEnv names the environment variable that turns Run into an
// OpenAPI dump. Set to a directory, it makes Run write the OpenAPI document of
// every API the service defines into that directory and end the process,
// without serving, connecting to anything, or starting a task:
//
//	DXLIB_OPENAPI_DUMP=out/openapi ./my-service
//
// It is how a tool reads a service's API surface from what dxlib itself
// emits, instead of from a per-service program that repeats main's wiring.
// Every service built on DXApp has it, with no code of its own.
const OpenAPIDumpEnv = "DXLIB_OPENAPI_DUMP"

// OpenAPIDumpFileSuffix is appended to an API's NameId to name its file in
// the dump directory: the "api" API is written to api.openapi.json.
const OpenAPIDumpFileSuffix = ".openapi.json"

// runOpenAPIDump is Run in dump mode. It runs only what defines the APIs and
// their endpoints:
//
//   - the vault clients are created (Start builds a client and dials nothing;
//     the definition hooks may read configuration through them);
//   - OnDefine and OnDefineConfiguration;
//   - the configuration is loaded, and the "api" configuration, if there is
//     one, creates its APIs as on a real start, TLS settings included, since
//     the TLS mode decides whether the document declares mutualTLS;
//   - OnDefineAPIEndPoints.
//
// It does not load or connect redis, the databases, object storage or the
// outbound HTTP client configuration, and does not run OnStartStorageReady,
// OnDefineSetVariables (a service may apply a schema there),
// OnAfterConfigurationStartAll or OnExecute. No listener is opened and no
// task started.
//
// It then writes <dir>/<NameId>.openapi.json for every API, each the bytes of
// DXAPI.OpenAPIAsJSON, and ends the process: status 0 when every file was
// written, 1 with the reason logged otherwise. It ends the process itself
// rather than returning to main, so that a service that discards Run's error
// still reports a failed dump, and nothing after Run in main runs.
func (a *DXApp) runOpenAPIDump(dir string) {
	if err := a.dumpOpenAPI(dir); err != nil {
		log.Log.Error(err.Error(), err)
		os.Exit(1)
	}
	os.Exit(0)
}

func (a *DXApp) dumpOpenAPI(dir string) (err error) {
	log.Log.Infof("%s set: writing the OpenAPI document of every API to %s and exiting", OpenAPIDumpEnv, dir)
	if a.InitVault != nil {
		if err = a.InitVault.Start(); err != nil {
			return err
		}
	}
	if a.EncryptionVault != nil {
		if err = a.EncryptionVault.Start(); err != nil {
			return err
		}
	}
	if a.OnDefine != nil {
		if err = a.OnDefine(); err != nil {
			return errors.Wrap(err, "ERROR_ON_DEFINE_CALLBACK")
		}
	}
	if a.OnDefineConfiguration != nil {
		if err = a.OnDefineConfiguration(); err != nil {
			return errors.Wrap(err, "ERROR_ON_DEFINE_CONFIGURATION_CALLBACK")
		}
	}
	if err = configuration.Manager.Load(); err != nil {
		return err
	}
	_, a.IsAPIExist = configuration.Manager.Configurations["api"]
	if a.IsAPIExist {
		if err = api.Manager.LoadFromConfiguration("api"); err != nil {
			return err
		}
	}
	if a.OnDefineAPIEndPoints != nil {
		if err = a.OnDefineAPIEndPoints(); err != nil {
			return err
		}
	}
	return writeOpenAPIDocuments(dir)
}

// writeOpenAPIDocuments writes one file per API in api.Manager, in NameId
// order. Every document is built before the first file is written, so an
// emitter error leaves the directory as it was.
func writeOpenAPIDocuments(dir string) error {
	nameIds := make([]string, 0, len(api.Manager.APIs))
	for nameId := range api.Manager.APIs {
		nameIds = append(nameIds, nameId)
	}
	sort.Strings(nameIds)
	documents := make([][]byte, len(nameIds))
	for i, nameId := range nameIds {
		b, err := api.Manager.APIs[nameId].OpenAPIAsJSON()
		if err != nil {
			return errors.Wrapf(err, "OPENAPI_DUMP:%s", nameId)
		}
		documents[i] = b
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return errors.Wrapf(err, "OPENAPI_DUMP_DIRECTORY:%s", dir)
	}
	for i, nameId := range nameIds {
		path := filepath.Join(dir, nameId+OpenAPIDumpFileSuffix)
		if err := os.WriteFile(path, documents[i], 0o644); err != nil {
			return errors.Wrapf(err, "OPENAPI_DUMP_WRITE:%s", path)
		}
		log.Log.Infof("OpenAPI dump: %s (%d endpoints)", path, len(api.Manager.APIs[nameId].EndPoints))
	}
	log.Log.Info(fmt.Sprintf("OpenAPI dump: %d documents written", len(nameIds)))
	return nil
}
