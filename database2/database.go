package database2

import (
	"context"
	"database/sql"

	"github.com/donnyhardyanto/dxlib/database2/database_type"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/pkg/errors"
	goOra "github.com/sijms/go-ora/v2"

	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/database2/sqlfile"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"

	"github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

type DXDatabaseEventFunc func(dm *DXDatabase, err error)

type DXDatabaseTx struct {
	*sqlx.Tx
	Log *log.DXLog
}
type DXDatabaseTxCallback func(dtx *DXDatabaseTx) (err error)

type DXDatabaseTxIsolationLevel = sql.IsolationLevel

const (
	LevelDefault DXDatabaseTxIsolationLevel = iota
	LevelReadUncommitted
	LevelReadCommitted
	LevelWriteCommitted
	LevelRepeatableRead
	LevelSnapshot
	LevelSerializable
	LevelLinearizable
)

type DXDatabase struct {
	NameId                       string
	IsConfigured                 bool
	DatabaseType                 database_type.DXDatabaseType
	Address                      string
	UserName                     string
	UserPassword                 string
	DatabaseName                 string
	ConnectionOptions            string
	IsConnectAtStart             bool
	MustConnected                bool
	Connected                    bool
	Connection                   *sqlx.DB
	ConnectionString             string
	NonSensitiveConnectionString string
	OnCannotConnect              DXDatabaseEventFunc
	CreateScriptFiles            []string
	ConcurrencySemaphore         chan struct{} // Adjust number based on your DB max_connections
}

func (d *DXDatabase) EnsureConnection() (err error) {
	if d.Connection == nil {
		err = d.Connect()
		if err != nil {
			return err
		}
	}
	if !d.Connected {
		err = d.Connect()
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DXDatabase) TransactionBegin(isolationLevel DXDatabaseTxIsolationLevel) (dtx *DXDatabaseTx, err error) {
	err = d.EnsureConnection()
	if err != nil {
		return nil, err
	}

	driverName := d.Connection.DriverName()
	switch driverName {
	case "oracle":
		tx, err := d.Connection.BeginTxx(context.Background(), &sql.TxOptions{
			ReadOnly: false,
		})
		if err != nil {
			return nil, err
		}
		dtx = &DXDatabaseTx{
			Tx:  tx,
			Log: &log.Log,
		}
		return dtx, nil
	}

	tx, err := d.Connection.BeginTxx(context.Background(), &sql.TxOptions{
		Isolation: isolationLevel,
		ReadOnly:  false,
	})
	if err != nil {
		return nil, err
	}
	dtx = &DXDatabaseTx{
		Tx:  tx,
		Log: &log.Log,
	}
	return dtx, nil
}

func (d *DXDatabase) CheckConnection() (err error) {
	err = d.EnsureConnection()
	if err != nil {
		return err
	}

	dbConn, err := d.Connection.Conn(context.Background())
	if err != nil {
		d.Connected = false
		return errors.Wrapf(err, "database %v CheckConnection() failed", d.NameId)
	}
	defer func() {
		_ = dbConn.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := dbConn.PingContext(ctx); err != nil {
		d.Connected = false
		return errors.Wrapf(err, "Database %v ping failed", d.NameId)
	}
	d.Connected = true
	return errors.Wrapf(err, "database %v ping success with result CheckConnection: %v", d.NameId)
}

func (d *DXDatabase) CheckConnectionAndReconnect() (err error) {
	tryReconnect := false
	if d.Connected {
		err = d.CheckConnection()
		if err != nil {
			tryReconnect = true
		}
		if !d.Connected {
			tryReconnect = true
		}
	} else {
		tryReconnect = true
	}
	if tryReconnect {
		time.Sleep(2 * time.Second)
		err = d.Connect()
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *DXDatabase) ExecuteScript(s *DXDatabaseScript) (err error) {
	err = d.EnsureConnection()
	if err != nil {
		return err
	}

	_, err = s.Execute(d)
	if err != nil {
		return err
	}
	return nil
}

func (d *DXDatabase) GetNonSensitiveConnectionString() string {
	return fmt.Sprintf("%s://%s/%s", d.DatabaseType.String(), d.Address, d.DatabaseName)
}

func (d *DXDatabase) GetConnectionString() (s string, err error) {
	switch d.DatabaseType {
	case database_type.PostgreSQL:
		host, portAsString, err := net.SplitHostPort(d.Address)
		if err != nil {
			return "", err
		}
		s = fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s %s", d.UserName, d.UserPassword, host, portAsString, d.DatabaseName, d.ConnectionOptions)
	case database_type.SQLServer:
		host, portAsString, err := net.SplitHostPort(d.Address)
		if err != nil {
			return "", err
		}
		s = fmt.Sprintf("server=%s;port=%s;user id=%s;password=%s;database=%s;%s", host, portAsString, d.UserName, d.UserPassword, d.DatabaseName, d.ConnectionOptions)
	case database_type.Oracle:
		host, portAsString, err := net.SplitHostPort(d.Address)
		if err != nil {
			return "", err
		}
		portInt, err := strconv.Atoi(portAsString)
		if err != nil {
			return "", err
		}
		urlOptions := map[string]string{
			//	"SERVICE_NAME": d.DatabaseName,
		}
		s = goOra.BuildUrl(host, portInt, d.DatabaseName, d.UserName, d.UserPassword, urlOptions)
	default:
		err = errors.Errorf("configuration is unusable, value of database_type field of database %s configuration is not supported (%s)", d.NameId, s)
	}
	return s, err
}

func (d *DXDatabase) ApplyFromConfiguration() (err error) {
	if !d.IsConfigured {
		log.Log.Infof("Configuring to Database %s... start", d.NameId)
		configurationData, ok := configuration.Manager.Configurations["storage"]
		if !ok {
			return errors.Errorf("storage configuration %s not found", d.NameId)
		}
		m := *(configurationData.Data)
		databaseConfiguration, ok := m[d.NameId].(utils.JSON)
		if !ok {
			if d.MustConnected {
				return errors.Errorf("database %s configuration not found", d.NameId)
			} else {
				return errors.Errorf("manager is unusable, database %s configuration not found", d.NameId)
			}
		}
		n, ok := databaseConfiguration["nameid"].(string)
		if ok {
			d.NameId = n
		}
		b, ok := databaseConfiguration["must_connected"].(bool)
		if ok {
			d.MustConnected = b
		}
		b, ok = databaseConfiguration["is_connect_at_start"].(bool)
		if ok {
			d.IsConnectAtStart = b
		}
		s, ok := databaseConfiguration["database_type"].(string)
		if !ok {
			if d.MustConnected {
				return errors.Errorf("mandatory database_type field value in database %s configuration is not supported (%v)", d.NameId, s)
			} else {
				return errors.Errorf("configuration is unusable, mandatory database_type field value database %s configuration  is not supported (%v)", d.NameId, s)
			}
		}
		d.DatabaseType = database_type.StringToDXDatabaseType(s)
		if d.DatabaseType == database_type.UnknownDatabaseType {
			if d.MustConnected {
				return errors.Errorf("mandatory value of database_type field of Database %s configuration is not supported (%s)", d.NameId, s)
			} else {
				return errors.Errorf("configuration is unusable, value of database_type field of database %s configuration is not supported (%s)", d.NameId, s)
			}
		}
		d.Address, ok = databaseConfiguration["address"].(string)
		if !ok {
			if d.MustConnected {
				return errors.Errorf("mandatory address field in Database %s configuration not exist", d.NameId)
			} else {
				return errors.Errorf("configuration is unusable, mandatory address field in database %s configuration not exist", d.NameId)
			}
		}
		d.UserName, ok = databaseConfiguration["user_name"].(string)
		if !ok {
			if d.MustConnected {
				return errors.Errorf("mandatory user_name field in Database %s configuration not exist", d.NameId)
			} else {
				return errors.Errorf("configuration is unusable, mandatory user_name field in Database %s configuration not exist", d.NameId)
			}
		}
		d.UserPassword, ok = databaseConfiguration["user_password"].(string)
		if !ok {
			if d.MustConnected {
				return errors.Errorf("mandatory user_password field in Database %s configuration not exist", d.NameId)
			} else {
				return errors.Errorf("configuration is unusable, mandatory user_password field in Database %s configuration not exist", d.NameId)
			}
		}
		d.DatabaseName, ok = databaseConfiguration["database_name"].(string)
		if !ok {
			if d.MustConnected {
				return errors.Errorf("mandatory database_name field in Database %s configuration not exist", d.NameId)
			} else {
				return errors.Errorf("configuration is unusable, mandatory database_name field in Database %s configuration not exist", d.NameId)
			}
		}
		d.CreateScriptFiles, _ = databaseConfiguration["create_script_files"].([]string)
		d.ConnectionOptions, _ = databaseConfiguration["connection_options"].(string)

		d.NonSensitiveConnectionString = d.GetNonSensitiveConnectionString()
		d.ConnectionString, err = d.GetConnectionString()
		if err != nil {
			return err
		}
		log.Log.Infof("Connecting to Database %s... done", d.NonSensitiveConnectionString)
		d.IsConfigured = true
		log.Log.Infof("Configuring to Database %s... done", d.NameId)
	}
	return nil
}

func (d *DXDatabase) CheckIsErrorBecauseDbNotExist(err error) bool {
	s := err.Error()
	switch d.DatabaseType {
	case database_type.PostgreSQL:
		t1 := strings.Contains(s, "database")
		t2 := strings.Contains(s, "not exist")
		t3 := strings.Contains(s, d.DatabaseName)
		if t1 && t2 && t3 {
			return true
		}
	default:
		return false
	}
	return false
}

func (d *DXDatabase) Connect() (err error) {
	if !d.Connected {
		log.Log.Infof("Connecting to database %s/%s... start", d.NameId, d.NonSensitiveConnectionString)
		connection, err := sqlx.Open(d.DatabaseType.Driver(), d.ConnectionString)
		if err != nil {
			if d.MustConnected {
				log.Log.Fatalf("Invalid parameters to open database %s/%s (%s)", d.NameId, d.NonSensitiveConnectionString, err.Error())
				return nil
			} else {
				return errors.Wrapf(err, "Invalid parameters to open database %s/%s", d.NameId, d.NonSensitiveConnectionString)
			}
		}
		d.Connection = connection
		err = connection.Ping()
		if err != nil {
			if d.OnCannotConnect != nil {
				d.OnCannotConnect(d, err)
			}
			if d.MustConnected {
				log.Log.Fatalf("Cannot connect and ping to database %s/%s (%s)", d.NameId, d.NonSensitiveConnectionString, err.Error())
				return nil
			} else {
				return errors.Wrapf(err, "Cannot connect and ping to database %s/%s", d.NameId, d.NonSensitiveConnectionString)
			}
		}
		d.Connected = true
		log.Log.Infof("Connecting to database %s/%s... done CONNECTED", d.NameId, d.NonSensitiveConnectionString)
	}
	return nil
}

func (d *DXDatabase) Disconnect() (err error) {
	if d.Connected {
		log.Log.Infof("Disconnecting to database %s/%s... start", d.NameId, d.NonSensitiveConnectionString)
		err := (*d.Connection).Close()
		if err != nil {
			return errors.Wrapf(err, "Disconnecting to database %s/%s error", d.NameId, d.NonSensitiveConnectionString)
		}
		d.Connection = nil
		d.Connected = false
		log.Log.Infof("Disconnecting to database %s/%s... done DISCONNECTED", d.NameId, d.NonSensitiveConnectionString)
	}
	return nil
}

func (d *DXDatabase) ExecuteFile(filename string) (r sql.Result, err error) {
	err = d.CheckConnectionAndReconnect()
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = errors.Wrapf(err, "Error executing file %s (%v)", filename)
		}
	}()

	driverName := d.Connection.DriverName()
	switch driverName {
	case "sqlserver", "postgres", "oracle":
		log.Log.Infof("Executing SQL file %s... start", filename)

		sqlFile := sqlfile.New()

		// Load a single file
		err = sqlFile.File(filename)
		if err != nil {
			return nil, err
		}

		// Execute the queries
		_, err = sqlFile.Exec(d.Connection.DB)
		if err != nil {
			return nil, err
		}

	default:
		log.Log.Fatalf("Driver %s is not supported", driverName)
		return nil, err
	}
	log.Log.Info("SQL script executed successfully!")
	return r, nil

}

func (d *DXDatabase) ExecuteCreateScripts() (rs []sql.Result, err error) {
	err = d.EnsureConnection()
	if err != nil {
		return nil, err
	}

	rs = []sql.Result{}
	for k, v := range d.CreateScriptFiles {
		r, err := d.ExecuteFile(v)
		if err != nil {
			log.Log.Errorf(err, "Error executing file %d:'%s' (%s)", k, v, err.Error())
			var sqlErr mssql.Error
			if errors.As(err, &sqlErr) {
				log.Log.Errorf(err, "SQL Server Error Number: %d, State: %d, Message: %s",
					sqlErr.Number, sqlErr.State, sqlErr.Message)
			}
			return rs, err
		}
		log.Log.Infof("Executing file %d:'%s'... done", k+1, v)
		rs = append(rs, r)
	}
	return rs, nil
}

func (d *DXDatabase) Tx(log *log.DXLog, isolationLevel sql.IsolationLevel, callback DXDatabaseTxCallback) (err error) {
	driverName := d.Connection.DriverName()
	switch driverName {
	case "oracle":
		tx, err := d.TransactionBegin(isolationLevel)
		if err != nil {
			return errors.Wrap(err, "error occured")
		}
		err = callback(tx)
		if err != nil {
			log.Errorf(err, "TX_ERROR_IN_CALLBACK: (%v)", err.Error())
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(errTx, "SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)", errTx.Error())
			}
			return errors.Wrap(err, "error occured")
		}
		err = tx.Commit()
		if err != nil {
			log.Errorf(err, "TX_ERROR_IN_COMMITT: (%v)", err.Error())
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(err, "ErrorInCommitRollback: (%v)", errTx.Error())
			}
			return errors.Wrap(err, "error occured")
		}

		return nil
	}

	tx, err := d.Connection.BeginTxx(log.Context, &sql.TxOptions{
		Isolation: isolationLevel,
		ReadOnly:  false,
	})
	if err != nil {
		return errors.Wrap(err, "error occured")
	}
	dtx := &DXDatabaseTx{
		Tx:  tx,
		Log: log,
	}
	err = callback(dtx)
	if err != nil {
		log.Errorf(err, "TX_ERROR_IN_CALLBACK: (%v)", err.Error())
		errTx := tx.Rollback()
		if errTx != nil {
			log.Errorf(err, "SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)", errTx.Error())
		}
		return errors.Wrap(err, "error occured")
	}
	err = dtx.Tx.Commit()
	if err != nil {
		log.Errorf(err, "TX_ERROR_IN_COMMIT: (%v)", err.Error())
		errTx := tx.Rollback()
		if errTx != nil {
			log.Errorf(err, "ErrorInCommitRollback: (%v)", errTx.Error())
		}
		return errors.Wrap(err, "error occured")
	}

	return nil
}
