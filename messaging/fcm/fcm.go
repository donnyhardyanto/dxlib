package fcm

import (
	"context"
	"sync"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"golang.org/x/time/rate"
	"google.golang.org/api/option"
)

/*
type Message struct {
	ID            int64
	ApplicationID int
	UserID        int
	Title         string
	Body          string
	Data          map[string]string
	Status        string
	RetryCount    int
	NextRetryTime time.Time
}*/

type FirebaseServiceAccount struct {
	App    *firebase.App
	Client *messaging.Client
}

type FirebaseAppManager struct {
	ServiceAccounts sync.Map
	Limiter         *rate.Limiter
}

func NewFirebaseAppManager() *FirebaseAppManager {
	return &FirebaseAppManager{
		Limiter: rate.NewLimiter(rate.Limit(500.0/60.0), 500), // 500 messages per minute
	}
}

func (fam *FirebaseAppManager) GetServiceAccount(applicationId int64) (*FirebaseServiceAccount, error) {
	firebaseApp, ok := fam.ServiceAccounts.Load(applicationId)
	if !ok {
		return nil, log.Log.ErrorAndCreateErrorf("SERVICE_ACCOUNT_NOT_FOUND:%d", applicationId)
	}
	return firebaseApp.(*FirebaseServiceAccount), nil
}

func (fam *FirebaseAppManager) StoreApplication(ctx context.Context, applicationId int64, serviceAccountData utils.JSON) (*FirebaseServiceAccount, error) {
	if firebaseApp, ok := fam.ServiceAccounts.Load(applicationId); ok {
		return firebaseApp.(*FirebaseServiceAccount), nil
	}

	serviceAccountJSON, err := utils.JSONToBytes(serviceAccountData)
	if err != nil {
		return nil, log.Log.ErrorAndCreateErrorf("failed to marshal service account data: %v", err)
	}

	firebaseApp, err := firebase.NewApp(ctx, nil, option.WithCredentialsJSON(serviceAccountJSON))
	if err != nil {
		return nil, log.Log.ErrorAndCreateErrorf("failed to create Firebase app: %v", err)
	}

	client, err := firebaseApp.Messaging(ctx)
	if err != nil {
		return nil, log.Log.ErrorAndCreateErrorf("failed to create Messaging client: %v", err)
	}

	newServiceAccount := &FirebaseServiceAccount{
		App:    firebaseApp,
		Client: client,
	}

	fam.ServiceAccounts.Store(applicationId, newServiceAccount)
	return newServiceAccount, nil
}

func (fam *FirebaseAppManager) RemoveApp(applicationId int64) {
	fam.ServiceAccounts.Delete(applicationId)
}

var Manager *FirebaseAppManager

func init() {
	Manager = NewFirebaseAppManager()
}
