package object_storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/api"
	dxlibv3Configuration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/core"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/donnyhardyanto/dxlib/errors"
)

type DXObjectStorageType int64

const (
	UnknownObjectStorageType DXObjectStorageType = iota
	Minio
)

func (t DXObjectStorageType) String() string {
	switch t {
	case Minio:
		return "minio"
	default:
		return "unknown"
	}
}

func StringToDXObjectStorageType(v string) DXObjectStorageType {
	switch v {
	case "minio":
		return Minio
	default:
		return UnknownObjectStorageType
	}
}

type DXObjectStorage struct {
	Owner             *DXObjectStorageManager
	NameId            string
	ObjectStorageType DXObjectStorageType
	IsConfigured      bool
	Address           string
	UserName          string
	HasUserName       bool
	Password          string
	HasPassword       bool
	BasePath          string
	UseSSL            bool
	BucketName        string
	IsConnectAtStart  bool
	MustConnected     bool
	Connected         bool
	Context           context.Context
	Client            *minio.Client
}

type DXObjectStorageManager struct {
	ObjectStorages map[string]*DXObjectStorage
}

func (osm *DXObjectStorageManager) NewObjectStorage(nameId string, isConnectAtStart, mustConnected bool) *DXObjectStorage {
	r := DXObjectStorage{
		Owner:            osm,
		NameId:           nameId,
		IsConfigured:     false,
		IsConnectAtStart: isConnectAtStart,
		MustConnected:    mustConnected,
		Connected:        false,
		HasUserName:      false,
		HasPassword:      false,
		BasePath:         "/",
		UseSSL:           false,
		Context:          core.RootContext,
	}
	osm.ObjectStorages[nameId] = &r
	return &r
}

func (osm *DXObjectStorageManager) LoadFromConfiguration(configurationNameId string) (err error) {
	configuration, ok := dxlibv3Configuration.Manager.Configurations[configurationNameId]
	if !ok {
		return errors.Errorf("CONFIGURATION_NOT_FOUND:%s", configurationNameId)
	}
	isConnectAtStart := false
	mustConnected := false
	for k, v := range *configuration.Data {
		d, ok := v.(utils.JSON)
		if !ok {
			err := log.Log.ErrorAndCreateErrorf("Cannot read %s as JSON", k)
			return err
		}
		isConnectAtStart, ok = d["is_connect_at_start"].(bool)
		if !ok {
			isConnectAtStart = false
		}
		mustConnected, ok = d["must_connected"].(bool)
		if !ok {
			mustConnected = false
		}
		ObjectStorageObject := osm.NewObjectStorage(k, isConnectAtStart, mustConnected)
		err := ObjectStorageObject.ApplyFromConfiguration()
		if err != nil {
			return err
		}
	}
	return nil
}

func (osm *DXObjectStorageManager) ConnectAllAtStart() (err error) {
	if len(osm.ObjectStorages) > 0 {
		log.Log.Info("Connecting to Database Manager... start")
		for _, v := range osm.ObjectStorages {
			err := v.ApplyFromConfiguration()
			if err != nil {
				err = log.Log.ErrorAndCreateErrorf("Cannot configure to database %s to connect", v.NameId)
				return err
			}
			if v.IsConnectAtStart {
				err = v.Connect()
				if err != nil {
					return err
				}
			}
		}
		log.Log.Info("Connecting to Database Manager... done")
	}

	return nil
}

func (osm *DXObjectStorageManager) ConnectAll() (err error) {
	for _, v := range osm.ObjectStorages {
		err := v.ApplyFromConfiguration()
		if err != nil {
			err = log.Log.ErrorAndCreateErrorf("Cannot configure to database %s to connect", v.NameId)
			return err
		}
		err = v.Connect()
		if err != nil {
			return err
		}
	}
	return nil
}

func (osm *DXObjectStorageManager) DisconnectAll() (err error) {
	for _, v := range osm.ObjectStorages {
		err = v.Disconnect()
		if err != nil {
			return err
		}
	}
	return nil
}

func (osm *DXObjectStorageManager) FindObjectStorageAndReceiveObject(aepr *api.DXAPIEndPointRequest, nameid string, filename string) (err error) {
	// Get the object storage objectStorage using the bucket_nameid
	objectStorage, exists := osm.ObjectStorages[nameid]
	if !exists {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "OBJECT_STORAGE_NAME_NOT_FOUND:%s", nameid)
	}

	err = objectStorage.ReceiveStreamObject(aepr, filename)
	if err != nil {
		return err
	}
	return nil
}

func (osm *DXObjectStorageManager) FindObjectStorageAndSendObject(aepr *api.DXAPIEndPointRequest, nameid string, filename string) (err error) {
	// Get the object storage objectStorage using the bucket_nameid
	objectStorage, exists := osm.ObjectStorages[nameid]
	if !exists {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "OBJECT_STORAGE_NAME_NOT_FOUND:%s", nameid)
	}

	err = objectStorage.SendStreamObject(aepr, filename)
	if err != nil {
		return err
	}
	return nil
}

func (r *DXObjectStorage) ApplyFromConfiguration() (err error) {
	if !r.IsConfigured {
		log.Log.Infof("Configuring to ObjectStorage %s... start", r.NameId)
		configurationData, ok := dxlibv3Configuration.Manager.Configurations["object_storage"]
		if !ok {
			err = log.Log.PanicAndCreateErrorf("DXObjectStorage/ApplyFromConfiguration/1", "ObjectStorage configuration not found")
			return err
		}
		m := *(configurationData.Data)
		ObjectStorageConfiguration, ok := m[r.NameId].(utils.JSON)
		if !ok {
			if r.MustConnected {
				err := log.Log.PanicAndCreateErrorf(log.Log.Prefix, "ObjectStorage %s configuration not found", r.NameId)
				return err
			} else {
				err := log.Log.WarnAndCreateErrorf("Manager is unusable, ObjectStorage %s configuration not found", r.NameId)
				return err
			}
		}
		r.Address, ok = ObjectStorageConfiguration["address"].(string)
		if !ok {
			if r.MustConnected {
				err := log.Log.PanicAndCreateErrorf(log.Log.Prefix, "Mandatory address field in ObjectStorage %s configuration not exist", r.NameId)
				return err
			} else {
				err := log.Log.WarnAndCreateErrorf("configuration is unusable, mandatory address field in ObjectStorage %s configuration not exist", r.NameId)
				return err
			}
		}
		r.UserName, r.HasUserName = ObjectStorageConfiguration["user_name"].(string)
		r.Password, r.HasPassword = ObjectStorageConfiguration["password"].(string)
		r.BucketName, ok = ObjectStorageConfiguration["bucket_name"].(string)
		if !ok {
			err := log.Log.ErrorAndCreateErrorf("Mandatory bucket_name field in object storage ObjectStorage %s configuration not exist.", r.NameId)
			return err
		}
		r.BasePath, ok = ObjectStorageConfiguration["base_path"].(string)
		r.UseSSL, ok = ObjectStorageConfiguration["use_ssl"].(bool)
		r.IsConfigured = true
		log.Log.Infof("Configuring to ObjectStorage %s... done", r.NameId)
	}
	return nil
}

var ObjectStorageMaxFileSizeBytes = 31 << 26

func (r *DXObjectStorage) Connect() (err error) {
	if !r.Connected {
		err := r.ApplyFromConfiguration()
		if err != nil {
			log.Log.Errorf(err, "Cannot configure to Object Storage %s to connect (%s)", r.NameId, err.Error())
			return err
		}
		log.Log.Infof("Connecting to Object Storage %s at %s/%s... start", r.NameId, r.Address, r.BucketName)

		minioClient, err := minio.New(
			r.Address,
			&minio.Options{
				Creds: credentials.NewStaticV4(
					r.UserName,
					r.Password,
					""),
				Secure: r.UseSSL,
			})
		if err != nil {
			return errors.Wrapf(err, "MINIO_CLIENT_CONNECT_ERROR:%s/%s/%s", r.NameId, r.Address, r.BucketName)
		}
		r.Client = minioClient
		r.Connected = true
		log.Log.Infof("Connecting to Object Storage %s at %s/%s... done CONNECTED", r.NameId, r.Address, r.BucketName)
	}
	return nil
}

func (r *DXObjectStorage) Disconnect() (err error) {
	if r.Connected {
		log.Log.Infof("Disconnecting to Object Storage %s at %s/%s... start", r.NameId, r.Address, r.BucketName)
		r.Client = nil
		r.Connected = false
		log.Log.Infof("Disconnecting to Object Storage %s at %s/%s... done DISCONNECTED", r.NameId, r.Address, r.BucketName)
	}
	return nil
}

func (r *DXObjectStorage) UploadStream(reader io.Reader, objectName string, originalFilename string, contentType string, disableMultipart bool, objectSize int64) (uploadInfo *minio.UploadInfo, err error) {
	if r.Client == nil {
		return nil, log.Log.ErrorAndCreateErrorf("CLIENT_IS_NIL")
	}
	fullPathObjectName := r.BasePath
	if !strings.HasSuffix(fullPathObjectName, "/") {
		fullPathObjectName += "/"
	}
	fullPathObjectName = fullPathObjectName + objectName
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// 1. Check if the bucket exists and create it if it doesn't
	exists, err := r.Client.BucketExists(ctx, r.BucketName)
	if err != nil && !exists {
		// If an error occurred (and the bucket definitely doesn't exist, which BucketExists handles)
		// or if the error is due to a connection issue, handle it here.
		// For simplicity, we proceed to create, but robust error checking may be needed.
	}

	if !exists {
		// Use the default region (MinIO's default is typically "us-east-1" but depends on setup)
		// Adjust minio.MakeBucketOptions{} if you need to specify a Region.
		err = r.Client.MakeBucket(ctx, r.BucketName, minio.MakeBucketOptions{})
		if err != nil {
			// Check if the bucket was created by another concurrent process
			exists, errCheck := r.Client.BucketExists(ctx, r.BucketName)
			if (errCheck != nil) || (!exists) {
				// Actual failure to create bucket
				return nil, log.Log.ErrorAndCreateErrorf("FAILED_TO_CREATE_BUCKET: %v", err)
			}
		}
		log.Log.Infof("Bucket '%s' created successfully.", r.BucketName)
	}

	tags := make(map[string]string)
	tags["original-filename"] = originalFilename

	info, err := r.Client.PutObject(
		ctx,
		r.BucketName,
		fullPathObjectName,
		reader,
		objectSize,
		minio.PutObjectOptions{
			ContentType:      contentType,
			DisableMultipart: disableMultipart,
			UserTags:         tags,
		})
	if err != nil {
		var err2 minio.ErrorResponse
		if errors.As(err, &err2) {
			// Log specific MinIO error details
			return nil, log.Log.ErrorAndCreateErrorf("MINIO_ERROR: %s - %s, Bucket:%s, FullPathObjectName:%s", err2.Code, err2.Message, r.BucketName, fullPathObjectName)
		}
		return nil, log.Log.ErrorAndCreateErrorf("UPLOAD_ERROR: %v", err)
	}
	return &info, nil
}

func (r *DXObjectStorage) ReceiveStreamObject(aepr *api.DXAPIEndPointRequest, filename string) (err error) {
	bodyLen := aepr.Request.ContentLength
	aepr.Log.Infof("Request body length: %d", bodyLen)

	s := aepr.Request.Body

	uploadInfo, err := r.UploadStream(s, filename, filename, "application/octet-stream", false, bodyLen)
	if err != nil {
		return err
	}
	aepr.Log.Infof("Upload info result: %v", uploadInfo)
	return nil
}

func (r *DXObjectStorage) DownloadStream(objectName string) (*minio.Object, error) {
	if r.Client == nil {
		return nil, log.Log.ErrorAndCreateErrorf("CLIENT_IS_NIL")
	}

	fullPathObjectName := r.BasePath
	if !strings.HasSuffix(fullPathObjectName, "/") {
		fullPathObjectName += "/"
	}
	fullPathObjectName = fullPathObjectName + objectName

	// Get the object from the bucket
	object, err := r.Client.GetObject(context.Background(), r.BucketName, fullPathObjectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "DOWNLOAD_STEAM_ERROR:%s/%s", r.BucketName, fullPathObjectName)
	}

	// Return the reader
	return object, nil
}

func (r *DXObjectStorage) SendStreamObject(aepr *api.DXAPIEndPointRequest, filename string) (err error) {
	// Get the object storage bucket using the bucket_name
	object, err := r.DownloadStream(filename)
	if err != nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusInternalServerError, "", "ERROR_IN_DOWNLOAD_STREAM:%s", err.Error())
	}
	if object == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusInternalServerError, "", "OBJECT_IS_NIL:%s", r.NameId)
	}

	objectInfo, err := object.Stat()
	if err != nil {
		return errors.Wrapf(err, "MINIO_OBJECT_STAT_ERROR:%s/%s/%s", r.NameId, r.BucketName, filename)
	}

	originalFilename, ok := objectInfo.UserMetadata["filename"]
	if !ok {
		originalFilename = filename
	}
	responseWriter := *aepr.GetResponseWriter()
	responseWriter.Header().Set("Content-Type", "application/octet-stream")
	responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", objectInfo.Size))
	if originalFilename != "" {
		responseWriter.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", originalFilename))
	}
	responseWriter.WriteHeader(http.StatusOK)
	aepr.ResponseStatusCode = http.StatusOK

	// Use io.Pipe to stream the object, the thread will exist until it send all the content, even after the handler return to web server
	reader, writer := io.Pipe()
	go func() {
		defer func() {
			_ = writer.Close()
		}()
		_, err := io.Copy(writer, object)
		if err != nil {
			aepr.Log.Errorf(err, "PIPE_COPY_ERROR: %s", err.Error())
		}
		_ = object.Close()
	}()

	// Send the object stream
	_, err = io.Copy(responseWriter, reader)
	if err != nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusInternalServerError, "", "SEND_STREAM_ERROR:%s", err.Error())
	}
	aepr.ResponseHeaderSent = true
	aepr.ResponseBodySent = true
	return nil
}

var Manager DXObjectStorageManager

func init() {
	Manager = DXObjectStorageManager{ObjectStorages: map[string]*DXObjectStorage{}}
}
