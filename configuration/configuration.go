package configuration

import (
	"encoding/json"
	"os"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/secure_memory"
	"gopkg.in/yaml.v3"

	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	json2 "github.com/donnyhardyanto/dxlib/utils/json"
)

type DXConfiguration struct {
	Owner            *DXConfigurationManager
	NameId           string
	Filename         string
	FileFormat       string
	MustExist        bool
	MustLoadFile     bool
	Data             *utils.JSON
	SensitiveDataKey []string
}

type DXConfigurationPrefixKeywordResolver = func(text string) (err error)

type DXConfigurationManager struct {
	Configurations map[string]*DXConfiguration
}

func (cm *DXConfigurationManager) GetConfigurationData(nameId string) (data *utils.JSON, err error) {
	c, ok := cm.Configurations[nameId]
	if !ok {
		err := log.Log.PanicAndCreateErrorf("DXConfigurationManager/GetConfigurationData", "CONFIGURATION_NOT_FOUND:%s", nameId)
		return nil, err
	}
	return c.Data, nil
}

func (cm *DXConfigurationManager) NewConfiguration(nameId string, filename string, fileFormat string, mustExist bool, mustLoadFile bool, data utils.JSON, sensitiveDataKey []string) *DXConfiguration {
	d := DXConfiguration{
		Owner:            cm,
		NameId:           nameId,
		Filename:         filename,
		FileFormat:       fileFormat,
		MustExist:        mustExist,
		MustLoadFile:     mustLoadFile,
		Data:             &data,
		SensitiveDataKey: sensitiveDataKey,
	}
	cm.Configurations[nameId] = &d
	return &d
}

func (cm *DXConfigurationManager) NewIfNotExistConfiguration(nameId string, filename string, fileFormat string, mustExist bool, mustLoadFile bool, data utils.JSON, sensitiveDataKey []string) *DXConfiguration {
	if _, ok := cm.Configurations[nameId]; ok {
		c := cm.Configurations[nameId]
		for k, v := range data {
			(*c.Data)[k] = v
		}
		return c
	}
	return cm.NewConfiguration(nameId, filename, fileFormat, mustExist, mustLoadFile, data, sensitiveDataKey)
}

func (c *DXConfiguration) ByteArrayJSONToJSON(v []byte) (r utils.JSON, err error) {
	err = json.Unmarshal(v, &r)
	return r, err
}

func (c *DXConfiguration) ByteArrayYAMLToJSON(v []byte) (r utils.JSON, err error) {
	err = yaml.Unmarshal(v, &r)
	return r, err
}

func (c *DXConfiguration) FilterSensitiveData() (r utils.JSON) {
	r = json2.Copy(*c.Data)

	// Mask any *SecureValue entries that survived the shallow copy
	maskSecureValuesInMap(r)

	// Apply automatic pattern-based masking first
	r = utils.MaskSensitiveDataInJSON(r)

	// Also apply explicit sensitive key list (for backwards compatibility and unique cases)
	// This ensures any keys not caught by pattern matching are still masked
	for _, v := range c.SensitiveDataKey {
		utils.SetValueInNestedMap(r, v, "********")
	}

	return r
}

func maskSecureValuesInMap(m map[string]any) {
	for k, v := range m {
		switch val := v.(type) {
		case *secure_memory.SecureValue:
			m[k] = "********[SECURE]"
		case map[string]any:
			maskSecureValuesInMap(val)
		default:
		}
	}
}

func (c *DXConfiguration) ShowToLog() {
	filteredData := c.FilterSensitiveData()
	dataAsString, err := json.MarshalIndent(filteredData, "", "  ")
	if err != nil {
		log.Log.Panic("DXConfiguration/ShowToLog/1", err)
		return
	}
	log.Log.Infof("%s=%s", c.NameId, dataAsString)
}

func (c *DXConfiguration) AsString() string {
	dataAsString, err := json.MarshalIndent(c.Data, "", "  ")
	if err != nil {
		log.Log.Panic("DXConfiguration/AsString/1", err)
		return ""
	}
	return c.NameId + ": " + string(dataAsString)
}

func (c *DXConfiguration) AsNonSensitiveString() string {
	filteredData := c.FilterSensitiveData()
	dataAsString, err := json.MarshalIndent(filteredData, "", "  ")
	if err != nil {
		log.Log.Panic("DXConfiguration/AsString/1", err)
		return ""
	}
	return c.NameId + ": " + string(dataAsString)
}
// GetString retrieves a string, transparently resolving *SecureValue.
// This is the primary getter — replaces GetSecureString as the canonical method.
func (c *DXConfiguration) GetString(dotPath string) (string, error) {
	value, err := utils.GetValueFromNestedMap(*c.Data, dotPath)
	if err != nil {
		return "", errors.Wrapf(err, "CONFIGURATION_GET_STRING_NOT_FOUND:%s:%s", c.NameId, dotPath)
	}
	if sv, ok := value.(*secure_memory.SecureValue); ok {
		return sv.Resolve()
	}
	s, ok := value.(string)
	if !ok {
		return "", errors.Errorf("CONFIGURATION_GET_STRING_TYPE_ERROR:%s:%s:got_%T", c.NameId, dotPath, value)
	}
	return s, nil
}

// GetSecureString is an alias for GetString (backward compatibility).
func (c *DXConfiguration) GetSecureString(dotPath string) (string, error) {
	return c.GetString(dotPath)
}

// GetInt retrieves an int value by dot-path.
func (c *DXConfiguration) GetInt(dotPath string) (int, error) {
	value, err := utils.GetValueFromNestedMap(*c.Data, dotPath)
	if err != nil {
		return 0, errors.Wrapf(err, "CONFIGURATION_GET_INT_NOT_FOUND:%s:%s", c.NameId, dotPath)
	}
	v, ok := value.(int)
	if !ok {
		return 0, errors.Errorf("CONFIGURATION_GET_INT_TYPE_ERROR:%s:%s:got_%T", c.NameId, dotPath, value)
	}
	return v, nil
}

// GetInt64 retrieves an int64 value by dot-path.
func (c *DXConfiguration) GetInt64(dotPath string) (int64, error) {
	value, err := utils.GetValueFromNestedMap(*c.Data, dotPath)
	if err != nil {
		return 0, errors.Wrapf(err, "CONFIGURATION_GET_INT64_NOT_FOUND:%s:%s", c.NameId, dotPath)
	}
	v, ok := value.(int64)
	if !ok {
		return 0, errors.Errorf("CONFIGURATION_GET_INT64_TYPE_ERROR:%s:%s:got_%T", c.NameId, dotPath, value)
	}
	return v, nil
}

// GetBool retrieves a bool value by dot-path.
func (c *DXConfiguration) GetBool(dotPath string) (bool, error) {
	value, err := utils.GetValueFromNestedMap(*c.Data, dotPath)
	if err != nil {
		return false, errors.Wrapf(err, "CONFIGURATION_GET_BOOL_NOT_FOUND:%s:%s", c.NameId, dotPath)
	}
	v, ok := value.(bool)
	if !ok {
		return false, errors.Errorf("CONFIGURATION_GET_BOOL_TYPE_ERROR:%s:%s:got_%T", c.NameId, dotPath, value)
	}
	return v, nil
}

// GetFloat64 retrieves a float64 value by dot-path.
func (c *DXConfiguration) GetFloat64(dotPath string) (float64, error) {
	value, err := utils.GetValueFromNestedMap(*c.Data, dotPath)
	if err != nil {
		return 0, errors.Wrapf(err, "CONFIGURATION_GET_FLOAT64_NOT_FOUND:%s:%s", c.NameId, dotPath)
	}
	v, ok := value.(float64)
	if !ok {
		return 0, errors.Errorf("CONFIGURATION_GET_FLOAT64_TYPE_ERROR:%s:%s:got_%T", c.NameId, dotPath, value)
	}
	return v, nil
}

// GetStringFromSubMap is a convenience for GetSecureString(subMapKey + "." + fieldKey).
func (c *DXConfiguration) GetStringFromSubMap(subMapKey, fieldKey string) (string, error) {
	return c.GetSecureString(subMapKey + "." + fieldKey)
}

func (c *DXConfiguration) LoadFromFile() (err error) {
	log.Log.Infof("Reading file %s... start", c.Filename)
	content, err := os.ReadFile(c.Filename)
	if err != nil {
		if c.MustExist {
			log.Log.Fatalf("Can not reading file %s, please check the file exists and has permission to be read. (%v)", c.Filename, err.Error())
			return errors.Wrap(err, "ERROR_IN_READING_FILE")
		}
		log.Log.Warnf("Can not reading file %s, please check the file exists and has permission to be read.", c.Filename)
		return errors.Wrap(err, "ERROR_IN_READING_FILE")
	}
	switch c.FileFormat {
	case "json":
		v, err := c.ByteArrayJSONToJSON(content)
		if err != nil {
			log.Log.Fatalf("Can not parsing file %s, please check the file content (%v)", c.Filename, err.Error())
			return errors.Wrap(err, "ERROR_IN_PARSING_FILE_CONTENT_JSON_TO_JSON")
		}
		*c.Data = json2.DeepMerge(v, *c.Data)
	case "yaml":
		v, err := c.ByteArrayYAMLToJSON(content)
		if err != nil {
			log.Log.Fatalf("Can not parsing file %s, please check the file content (%v)", c.Filename, err.Error())
			return errors.Wrap(err, "ERROR_IN_PARSING_FILE_CONTENT_YAML_TO_JSON")
		}
		*c.Data = json2.DeepMerge(v, *c.Data)
	default:
		err = log.Log.PanicAndCreateErrorf("DXConfiguration/Load/1", "unknown file format: %s", c.FileFormat)
		return err
	}
	log.Log.Infof("Reading file %s... done", c.Filename)
	return nil
}

func (c *DXConfiguration) WriteToFile() (err error) {
	return nil
}

func (cm *DXConfigurationManager) ShowToLog() (err error) {
	for _, v := range cm.Configurations {
		v.ShowToLog()
	}
	return nil
}

func (cm *DXConfigurationManager) AsString() (s string) {
	s = ""
	for _, v := range cm.Configurations {
		s = s + v.AsString() + "\n"
	}
	return s
}
func (cm *DXConfigurationManager) AsNonSensitiveString() (s string) {
	s = ""
	for _, v := range cm.Configurations {
		s = s + v.AsNonSensitiveString() + "\n"
	}
	return s
}
func (cm *DXConfigurationManager) Load() (err error) {
	if len(cm.Configurations) > 0 {
		log.Log.Info("Reading configuration file(s)...")
		for _, v := range cm.Configurations {
			if v.MustLoadFile {
				_ = v.LoadFromFile()
			}
		}
		log.Log.Infof("Manager=\n%v", Manager.AsNonSensitiveString())
	}
	return nil
}

var Manager DXConfigurationManager

func init() {
	Manager = DXConfigurationManager{
		Configurations: map[string]*DXConfiguration{},
	}
}
