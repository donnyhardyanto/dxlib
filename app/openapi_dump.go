package app

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/databases/models"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
)

// OpenAPIDumpEnv names the environment variable that turns Run into an
// OpenAPI dump. Set to a directory, it makes Run write the OpenAPI document of
// every API the service defines, and the catalogue of every data model in
// DXApp.ModelDBs, into that directory and end the process, without serving,
// connecting to anything, or starting a task:
//
//	DXLIB_OPENAPI_DUMP=out/openapi ./my-service
//
// It is how a tool reads a service's API surface and declared data model from
// what dxlib itself emits, instead of from a per-service program that repeats
// main's wiring.
// Every service built on DXApp has it, with no code of its own.
const OpenAPIDumpEnv = "DXLIB_OPENAPI_DUMP"

// OpenAPIDumpFileSuffix is appended to an API's NameId to name its file in
// the dump directory: the "api" API is written to api.openapi.json.
const OpenAPIDumpFileSuffix = ".openapi.json"

// CatalogueDumpFileSuffix is appended to a model's Name to name its catalogue
// in the dump directory: the "core" model is written to core.catalogue.json.
const CatalogueDumpFileSuffix = ".catalogue.json"

// CatalogueDumpPathEnv and CatalogueDumpCommitEnv give the "path" and
// "commit" members of every catalogue the dump writes: the source the models
// are declared in, from the repository's root, and the commit that last
// changed it. The service cannot know either, so the tool that runs the dump
// sets them; unset, both are written as "".
const (
	CatalogueDumpPathEnv   = "DXLIB_CATALOGUE_PATH"
	CatalogueDumpCommitEnv = "DXLIB_CATALOGUE_COMMIT"
)

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
// DXAPI.OpenAPIAsJSON, and <dir>/<Name>.catalogue.json for every model in
// ModelDBs, each the bytes of models.ModelDB.CatalogueAsJSON with the path and
// commit of CatalogueDumpPathEnv and CatalogueDumpCommitEnv. A model is seen
// only when ModelDBs holds it by the time OnDefineAPIEndPoints has returned:
// set it before Run, or in OnDefine, OnDefineConfiguration or
// OnDefineAPIEndPoints, not in a hook the dump skips. It ends the process:
// status 0 when every file was written, 1 with the reason logged otherwise. It ends the process itself
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
	return writeDumpDocuments(dir, a.ModelDBs, os.Getenv(CatalogueDumpPathEnv), os.Getenv(CatalogueDumpCommitEnv))
}

type dumpDocument struct {
	file  string
	bytes []byte
	what  string
}

// writeDumpDocuments writes one file per API in api.Manager, in NameId
// order, then one per model, in Name order. Every document is built before
// the first file is written, so an emitter error leaves the directory as it
// was.
func writeDumpDocuments(dir string, modelDBs []*models.ModelDB, cataloguePath, catalogueCommit string) error {
	nameIds := make([]string, 0, len(api.Manager.APIs))
	for nameId := range api.Manager.APIs {
		nameIds = append(nameIds, nameId)
	}
	sort.Strings(nameIds)
	var documents []dumpDocument
	for _, nameId := range nameIds {
		b, err := api.Manager.APIs[nameId].OpenAPIAsJSON()
		if err != nil {
			return errors.Wrapf(err, "OPENAPI_DUMP:%s", nameId)
		}
		documents = append(documents, dumpDocument{nameId + OpenAPIDumpFileSuffix, b, fmt.Sprintf("%d endpoints", len(api.Manager.APIs[nameId].EndPoints))})
	}

	sorted := make([]*models.ModelDB, 0, len(modelDBs))
	byName := map[string]bool{}
	for _, m := range modelDBs {
		if m == nil {
			continue
		}
		if m.Name == "" || m.Name != filepath.Base(m.Name) || m.Name == "." || m.Name == ".." {
			return errors.Errorf("CATALOGUE_DUMP_MODEL_NAME_NOT_A_FILE_NAME:%q", m.Name)
		}
		if byName[m.Name] {
			return errors.Errorf("CATALOGUE_DUMP_DUPLICATE_MODEL_NAME:%s", m.Name)
		}
		byName[m.Name] = true
		sorted = append(sorted, m)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	for _, m := range sorted {
		b, err := m.CatalogueAsJSON(cataloguePath, catalogueCommit)
		if err != nil {
			return errors.Wrapf(err, "CATALOGUE_DUMP:%s", m.Name)
		}
		tables := 0
		for _, s := range m.Schemas {
			tables += len(s.Tables)
		}
		documents = append(documents, dumpDocument{m.Name + CatalogueDumpFileSuffix, b, fmt.Sprintf("%d tables", tables)})
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return errors.Wrapf(err, "OPENAPI_DUMP_DIRECTORY:%s", dir)
	}
	for _, d := range documents {
		path := filepath.Join(dir, d.file)
		if err := os.WriteFile(path, d.bytes, 0o644); err != nil {
			return errors.Wrapf(err, "OPENAPI_DUMP_WRITE:%s", path)
		}
		log.Log.Infof("OpenAPI dump: %s (%s)", path, d.what)
	}
	log.Log.Info(fmt.Sprintf("OpenAPI dump: %d documents written", len(documents)))
	return nil
}
