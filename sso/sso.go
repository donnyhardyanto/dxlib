package sso

import (
	"encoding/base64"
	"fmt"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/redis"
	"github.com/donnyhardyanto/dxlib/utils"
	json2 "github.com/donnyhardyanto/dxlib/utils/json"
	"github.com/golang-jwt/jwt/v5"
	"time"
	_ "time/tzdata"
)

var OrganizationManager DXOrganizationsManager

type DXOrganization struct {
	NameId                           string
	Name                             string
	HMACSecret                       string
	AuthenticationMethod             string
	DatabaseNameId                   string
	DatabaseTableUser                string
	DatabaseTableUserFieldUserId     string
	DatabaseTableUserFieldUserLogin  string
	DatabaseTablePassword            string
	DatabaseTablePasswordFieldUserId string
	DatabaseTablePasswordFieldValue  string

	RemoteServiceLoginRequestUrl                   string
	RemoteServiceLoginRequestMethod                string
	RemoteServiceLoginRequestPayload               utils.JSON
	RemoteServiceLoginResponseFieldPathStatus      string
	RemoteServiceLoginResponseFieldStatusIfSuccess string
	RemoteServiceLoginResponseFieldPathAccessToken string
	RemoteServiceLoginResponseFieldPathData        string

	RemoteServiceUserViewRequestUrl                   string
	RemoteServiceUserViewRequestMethod                string
	RemoteServiceUserViewRequestPayload               utils.JSON
	RemoteServiceUserViewResponseFieldPathStatus      string
	RemoteServiceUserViewResponseFieldStatusIfSuccess string
	RemoteServiceUserViewResponseFieldPathAccessToken string
	RemoteServiceUserViewResponseFieldPathData        string

	RemoteServiceProxyRequestUrl                    string `json:"remote_service_proxy_request_url"`
	RemoteServiceProxyRequestMethod                 string `json:"remote_service_proxy_request_method"`
	RemoteServiceProxyRequestFieldPathUserLoginData string `json:"remote_service_proxy_request_field_path_user_login_data"`
	RemoteServiceProxyRequestFieldPathPayload       string `json:"remote_service_proxy_request_field_path_payload"`

	RemoteServiceProxyResponseFieldPathStatus        string `json:"remote_service_proxy_response_field_path_status"`
	RemoteServiceProxyResponseFieldStatusIfSuccess   string `json:"remote_service_proxy_response_field_status_if_success"`
	RemoteServiceProxyResponseFieldPathAccessToken   string `json:"remote_service_proxy_response_field_path_access_token"`
	RemoteServiceProxyResponseFieldPathUserLoginData string `json:"remote_service_proxy_response_field_path_user_login_data"`
	RemoteServiceProxyResponseFieldPathResponse      string `json:"remote_service_proxy_response_field_path_response"`

	RemoteServiceProfileUrl               string
	RemoteServiceProfileUrlMethod         string
	RemoteServiceProfileUrlMapFieldUserId string
	RedisNameId                           string
	AccessTokenTimeoutDurationSec         int64
	Applications                          utils.JSON
	Database                              *database.DXDatabase
	Redis                                 *redis.DXRedis
}

type DXOrganizationsManager struct {
	Organizations map[string]*DXOrganization
}

func (om *DXOrganizationsManager) NewOrganization(nameid string) *DXOrganization {
	org := DXOrganization{
		NameId: nameid,
	}
	om.Organizations[nameid] = &org
	return &org
}

func (om *DXOrganizationsManager) GetValidOrganizationAndApplication(organizationNameId, applicationNameid string) (org *DXOrganization, applicationSettings utils.JSON, err error) {
	organization, ok := om.Organizations[organizationNameId]
	if !ok {
		err = errors.Errorf("invalid Organization nameId %s", organizationNameId)
		return nil, nil, err
	}
	applicationSettings, ok = (*organization).Applications[applicationNameid].(utils.JSON)
	if !ok {
		err = errors.Errorf("invalid FCMApplication nameId %s for Organization %s", applicationNameid, organizationNameId)
		return nil, nil, err
	}
	return organization, applicationSettings, nil
}

func (o *DXOrganization) ApplyData(d utils.JSON) (err error) {
	o.NameId = d[`nameid`].(string)
	o.Name = d[`name`].(string)
	o.HMACSecret = d[`hmac_secret`].(string)
	o.RedisNameId = d[`redis_nameid`].(string)
	o.AuthenticationMethod = d[`authentication_method`].(string)
	switch o.AuthenticationMethod {
	case "db_password":
		o.DatabaseNameId = d[`database_nameid`].(string)
		o.DatabaseTableUser = d[`database_table_user`].(string)
		o.DatabaseTableUserFieldUserLogin = d[`database_table_user_field_user_login`].(string)
		o.DatabaseTableUserFieldUserId = d[`database_table_user_field_user_login`].(string)
		o.DatabaseTablePassword = d[`database_table_password`].(string)
		o.DatabaseTablePasswordFieldUserId = d[`database_table_password_field_user_id`].(string)
		o.DatabaseTablePasswordFieldValue = d[`database_table_password_field_value`].(string)
	case "url":
		o.RemoteServiceLoginRequestUrl = d[`remote_service_login_request_url`].(string)
		o.RemoteServiceLoginRequestMethod = d[`remote_service_login_request_method`].(string)
		o.RemoteServiceLoginRequestPayload = d[`remote_service_login_request_payload`].(utils.JSON)
		o.RemoteServiceLoginResponseFieldPathStatus = d[`remote_service_login_response_field_path_status`].(string)
		o.RemoteServiceLoginResponseFieldStatusIfSuccess = d[`remote_service_login_response_field_status_if_success`].(string)
		o.RemoteServiceLoginResponseFieldPathAccessToken = d[`remote_service_login_response_field_path_access_token`].(string)
		o.RemoteServiceLoginResponseFieldPathData = d[`remote_service_login_response_field_path_data`].(string)

		o.RemoteServiceUserViewRequestUrl = d[`remote_service_user_view_request_url`].(string)
		o.RemoteServiceUserViewRequestMethod = d[`remote_service_user_view_request_method`].(string)
		o.RemoteServiceUserViewRequestPayload = d[`remote_service_user_view_request_payload`].(utils.JSON)
		o.RemoteServiceUserViewResponseFieldPathStatus = d[`remote_service_user_view_response_field_path_status`].(string)
		o.RemoteServiceUserViewResponseFieldStatusIfSuccess = d[`remote_service_user_view_response_field_status_if_success`].(string)
		o.RemoteServiceUserViewResponseFieldPathData = d[`remote_service_user_view_response_field_path_data`].(string)
	}

	o.Applications = d[`applications`].(utils.JSON)
	o.Database = database.Manager.Databases[o.DatabaseNameId]
	o.Redis = redis.Manager.Redises[o.RedisNameId]
	o.AccessTokenTimeoutDurationSec, err = json2.GetInt64(d, `access_token_timeout_duration_sec`)
	if err != nil {
		err := log.Log.PanicAndCreateErrorf("DXOrganization/ApplyData", "Can not case AccessTokenTimeoutDurationSec tp Int64 (%v)", err.Error())
		return errors.Wrap(err, "error occured")
	}
	return nil
}

func GenerateAccessToken(hmacSecret string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"data1": utils.RandomData(256),
		"nbf":   time.Now().Unix(),
	})
	tokenString, err := token.SignedString([]byte(hmacSecret))
	return tokenString, err
}

func GenerateAPIKey(hmacSecret string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"data1": utils.RandomData(256),
		"nbf":   time.Now().Unix(),
	})
	tokenString, err := token.SignedString([]byte(hmacSecret))
	h := jwt.SigningMethodHS512.Hash.New()
	h.Write([]byte(tokenString))
	key := base64.RawStdEncoding.EncodeToString(h.Sum(nil))
	return key, err
}

func init() {
	OrganizationManager = DXOrganizationsManager{
		Organizations: map[string]*DXOrganization{},
	}
}
