package jwt

import (
	"github.com/golang-jwt/jwt/v5"
)

func Validate(jwtTokenAsString string, validationKeyAsString string) (isValid bool, err error) {
	claims := jwt.RegisteredClaims{}
	p := jwt.NewParser()
	var token *jwt.Token
	token, err = p.ParseWithClaims(jwtTokenAsString, &claims, func(aToken *jwt.Token) (interface{}, error) {
		validationKey := []byte(validationKeyAsString)
		return validationKey, nil
	})
	if err != nil {
		return false, err
	}
	if !token.Valid {
		return false, nil
	}
	return true, nil
}
