package simple

import (
	"github.com/donnyhardyanto/dxlib/redis"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/google/uuid"
	"strings"
	"time"
)

func Login(redis *redis.DXRedis, credentials []utils.JSON, key string, secret string, data utils.JSON, ttlSecond int) (isSuccess bool, sessionKey string, err error) {
	for _, credential := range credentials {
		cKey, ok := credential["key"].(string)
		if !ok {
			continue
		}
		if cKey == key {
			cSecret := credential["secret"].(string)
			if cSecret == secret {
				a, err := uuid.NewV7()
				if err != nil {
					return false, "", err
				}
				b, err := uuid.NewRandom()
				if err != nil {
					return false, "", err
				}
				c := a.String() + b.String()
				sessionKey := strings.ReplaceAll(c, "-", "")

				sessionKeyTTLAsDuration := time.Duration(ttlSecond) * time.Second
				sessionObject := utils.JSON{
					"key":  key,
					"data": data,
				}
				err = redis.Set(sessionKey, sessionObject, sessionKeyTTLAsDuration)
				if err != nil {
					return false, "", err
				}

				return true, sessionKey, nil
			}
		}

	}
	return false, "", nil
}

func Authenticate(redis *redis.DXRedis, session string, ttlSecond int) (isSuccess bool, data utils.JSON, err error) {
	sessionKeyTTLAsDuration := time.Duration(ttlSecond) * time.Second

	sessionObject, err := redis.GetEx(session, sessionKeyTTLAsDuration)
	if err != nil {
		return false, nil, err
	}
	if sessionObject == nil {
		return false, sessionObject, nil
	}
	return true, sessionObject, nil
}
