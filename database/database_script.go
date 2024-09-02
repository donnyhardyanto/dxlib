package database

import (
	"database/sql"

	"dxlib/v3/database/sqlfile"
	"dxlib/v3/log"
)

type DXDatabaseScript struct {
	Owner              *DXDatabaseManager
	NameId             string
	ManagementDatabase *DXDatabase
	Files              []string
}

func (dm *DXDatabaseManager) NewDatabaseScript(nameId string, files []string) *DXDatabaseScript {
	ds := DXDatabaseScript{
		Owner:  dm,
		NameId: nameId,
		Files:  files,
	}
	dm.Scripts[nameId] = &ds
	return &ds
}

func (ds *DXDatabaseScript) ExecuteFile(d *DXDatabase, filename string) (r sql.Result, err error) {
	log.Log.Infof("Executing SQL file %s... start", filename)
	fs := sqlfile.SqlFile{}
	err = fs.File(filename)
	if err != nil {
		log.Log.Panic("DXDatabaseScript/ExecuteFile/1", err)
		return nil, err
	}
	rs, err := fs.Exec(d.Connection.DB)
	if err != nil {
		log.Log.Fatalf("Error executing SQL file %s (%v)", filename, err.Error())
		return rs[0], err
	}
	log.Log.Infof("Executing SQL file %s... done", filename)
	return rs[0], nil
}

func (ds *DXDatabaseScript) Execute(d *DXDatabase) (rs []sql.Result, err error) {
	rs = []sql.Result{}
	for k, v := range ds.Files {
		r, err := ds.ExecuteFile(d, v)
		if err != nil {
			log.Log.Errorf("Error executing file %d:'%s' (%err)", k, v, err.Error())
			return rs, err
		}
		rs = append(rs, r)
	}
	return rs, nil
}
