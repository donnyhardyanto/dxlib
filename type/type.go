package _type

type TypeCompatibilityMappingStruct struct {
	APIParameterType string
	JSONType         string
	GoType           string
	DbTypePostgreSQL string
	DbTypeSqlserver  string
	DbTypeMysql      string
	DbTypeOracle     string
}

var Types []TypeCompatibilityMappingStruct

type TModel struct {
}

func init() {
	Types = []TypeCompatibilityMappingStruct{}
	var a TypeCompatibilityMappingStruct

	a = TypeCompatibilityMappingStruct{
		APIParameterType: "string",
		GoType:           "string",
		DbTypePostgreSQL: "VARCHAR(1024)",
		DbTypeSqlserver:  "VARCHAR(1024)",
		DbTypeMysql:      "VARCHAR(1024)",
		DbTypeOracle:     "VARCHAR(1024)",
	}
	Types = append(Types, a)

	a = TypeCompatibilityMappingStruct{
		APIParameterType: "int",
		GoType:           "int",
		DbTypePostgreSQL: "INT",
		DbTypeSqlserver:  "INT",
		DbTypeMysql:      "INT",
		DbTypeOracle:     "INT",
	}
	Types = append(Types, a)

	a = TypeCompatibilityMappingStruct{
		APIParameterType: "bool",
		GoType:           "bool",
		DbTypePostgreSQL: "BOOLEAN",
		DbTypeSqlserver:  "BIT",
		DbTypeMysql:      "BOOLEAN",
		DbTypeOracle:     "NUMBER(1)",
	}
	Types = append(Types, a)

	a = TypeCompatibilityMappingStruct{
		APIParameterType: "float64",
		GoType:           "float64",
		DbTypePostgreSQL: "FLOAT",
		DbTypeSqlserver:  "FLOAT",
		DbTypeMysql:      "FLOAT",
		DbTypeOracle:     "FLOAT",
	}
	Types = append(Types, a)

	a = TypeCompatibilityMappingStruct{
		APIParameterType: "[]byte",
		GoType:           "[]byte",
		DbTypePostgreSQL: "BYTEA",
		DbTypeSqlserver:  "IMAGE",
		DbTypeMysql:      "BLOB",
		DbTypeOracle:     "BLOB",
	}
	Types = append(Types, a)

	a = TypeCompatibilityMappingStruct{
		APIParameterType: "time.Time",
		GoType:           "time.Time",
		DbTypePostgreSQL: "TIMESTAMP",
		DbTypeSqlserver:  "DATETIME",
		DbTypeMysql:      "DATETIME",
		DbTypeOracle:     "TIMESTAMP",
	}
}
