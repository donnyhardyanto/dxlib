package _type

type TypeCompatibiliyMappingStruct struct {
	Api_parameter_type string
	Go_type            string
	Db_type_postgres   string
	Db_type_sqlserver  string
	Db_type_mysql      string
	Db_type_oracle     string
}

var Types []TypeCompatibiliyMappingStruct

func init() {
	Types = []TypeCompatibiliyMappingStruct{}
	var a TypeCompatibiliyMappingStruct

	a = TypeCompatibiliyMappingStruct{
		Api_parameter_type: "string",
		Go_type:            "string",
		Db_type_postgres:   "VARCHAR(1024)",
		Db_type_sqlserver:  "VARCHAR(1024)",
		Db_type_mysql:      "VARCHAR(1024)",
		Db_type_oracle:     "VARCHAR(1024)",
	}
	Types = append(Types, a)

	a = TypeCompatibiliyMappingStruct{
		Api_parameter_type: "int",
		Go_type:            "int",
		Db_type_postgres:   "INT",
		Db_type_sqlserver:  "INT",
		Db_type_mysql:      "INT",
		Db_type_oracle:     "INT",
	}
	Types = append(Types, a)

	a = TypeCompatibiliyMappingStruct{
		Api_parameter_type: "bool",
		Go_type:            "bool",
		Db_type_postgres:   "BOOLEAN",
		Db_type_sqlserver:  "BIT",
		Db_type_mysql:      "BOOLEAN",
		Db_type_oracle:     "NUMBER(1)",
	}
	Types = append(Types, a)

	a = TypeCompatibiliyMappingStruct{
		Api_parameter_type: "float64",
		Go_type:            "float64",
		Db_type_postgres:   "FLOAT",
		Db_type_sqlserver:  "FLOAT",
		Db_type_mysql:      "FLOAT",
		Db_type_oracle:     "FLOAT",
	}
	Types = append(Types, a)

	a = TypeCompatibiliyMappingStruct{
		Api_parameter_type: "[]byte",
		Go_type:            "[]byte",
		Db_type_postgres:   "BYTEA",
		Db_type_sqlserver:  "IMAGE",
		Db_type_mysql:      "BLOB",
		Db_type_oracle:     "BLOB",
	}
	Types = append(Types, a)

	a = TypeCompatibiliyMappingStruct{
		Api_parameter_type: "time.Time",
		Go_type:            "time.Time",
		Db_type_postgres:   "TIMESTAMP",
		Db_type_sqlserver:  "DATETIME",
		Db_type_mysql:      "DATETIME",
		Db_type_oracle:     "TIMESTAMP",
	}

}
