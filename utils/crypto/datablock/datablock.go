package datablock

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha512"
	"dxlib/v3/utils"
	"dxlib/v3/utils/crypto/aes"
	"dxlib/v3/utils/lv"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"time"
)

type DataBlock struct {
	Time     lv.LV
	Nonce    lv.LV
	PreKey   lv.LV
	Data     lv.LV
	DataHash lv.LV
}

func NewDataBlock(data []byte) (*DataBlock, error) {
	b := &DataBlock{
		Time:     lv.LV{},
		Nonce:    lv.LV{},
		PreKey:   lv.LV{},
		Data:     lv.LV{},
		DataHash: lv.LV{},
	}
	err := b.SetTimeNow()
	if err != nil {
		return nil, err
	}
	err = b.GenerateNonce()
	if err != nil {
		return nil, err
	}
	err = b.SetDataValue(data)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (db *DataBlock) SetTimeNow() error {
	err := db.Time.SetValue(time.Now().UnixNano())
	if err != nil {
		return err
	}
	return nil
}

func (db *DataBlock) GenerateNonce() (err error) {
	err = db.Nonce.SetValue(utils.RandomData(32))
	if err != nil {
		return err
	}
	return nil
}

func (db *DataBlock) SetDataValue(data any) (err error) {
	err = db.Data.SetValue(data)
	if err != nil {
		return err
	}
	err = db.GenerateDataHash()
	if err != nil {
		return err
	}
	return nil
}

func (db *DataBlock) GenerateDataHash() (err error) {
	dataAsBytes := db.Data.Value
	x := sha512.Sum512(dataAsBytes)
	err = db.DataHash.SetValue(x[:])
	if err != nil {
		return err
	}
	return nil
}

func (db *DataBlock) CheckDataHash() bool {
	dataAsBytes := db.Data.Value
	dataHashAsBytes := db.DataHash.Value
	x := sha512.Sum512(dataAsBytes)
	return bytes.Equal(dataHashAsBytes, x[:])
}

/*
	func (db *DataBlock) AsLV() (r *lv.LV, err error) {
		r, err = lv.CombineLV(&db.Time, &db.Nonce, &db.PreKey, &db.Data, &db.DataHash)
		if err != nil {
			return nil, err
		}
		return r, nil
	}
*/
func (db *DataBlock) MarshalBinary() (b []byte, err error) {
	valueAsBuffer := new(bytes.Buffer)

	timeAsBytes, err := db.Time.MarshalBinary()
	if err != nil {
		return nil, err
	}
	err = binary.Write(valueAsBuffer, binary.BigEndian, timeAsBytes)
	if err != nil {
		return nil, err
	}

	nonceAsBytes, err := db.Nonce.MarshalBinary()
	if err != nil {
		return nil, err
	}
	err = binary.Write(valueAsBuffer, binary.BigEndian, nonceAsBytes)
	if err != nil {
		return nil, err
	}

	preKeyAsBytes, err := db.PreKey.MarshalBinary()
	if err != nil {
		return nil, err
	}
	err = binary.Write(valueAsBuffer, binary.BigEndian, preKeyAsBytes)
	if err != nil {
		return nil, err
	}

	dataAsBytes, err := db.Data.MarshalBinary()
	if err != nil {
		return nil, err
	}
	err = binary.Write(valueAsBuffer, binary.BigEndian, dataAsBytes)
	if err != nil {
		return nil, err
	}

	dataHashAsBytes, err := db.DataHash.MarshalBinary()
	if err != nil {
		return nil, err
	}
	err = binary.Write(valueAsBuffer, binary.BigEndian, dataHashAsBytes)
	if err != nil {
		return nil, err
	}

	return valueAsBuffer.Bytes(), nil
}

func (db *DataBlock) UnmarshalBinaryFromReader(r *bytes.Reader) (err error) {
	err = db.Time.UnmarshalBinaryFromReader(r)
	if err != nil {
		return err
	}
	err = db.Nonce.UnmarshalBinaryFromReader(r)
	if err != nil {
		return err
	}
	err = db.PreKey.UnmarshalBinaryFromReader(r)
	if err != nil {
		return err
	}
	err = db.Data.UnmarshalBinaryFromReader(r)
	if err != nil {
		return err
	}
	err = db.DataHash.UnmarshalBinaryFromReader(r)
	if err != nil {
		return err
	}
	return nil
}

func (db *DataBlock) UnmarshalBinary(data []byte) (err error) {
	r := bytes.NewReader(data)
	err = db.UnmarshalBinaryFromReader(r)
	if err != nil {
		return err
	}
	return nil
}

func PackLVPayload(preKeyIndex string, edSelfPrivateKey []byte, encryptKey []byte, payloads ...*lv.LV) (r string, err error) {
	lvPackedPayload, err := lv.CombineLV(payloads...)
	if err != nil {
		return "", err
	}
	lvPackedPayloadAsBytes, err := lvPackedPayload.MarshalBinary()
	if err != nil {
		return "", err
	}

	dataBlock, err := NewDataBlock(lvPackedPayloadAsBytes)
	if err != nil {
		return "", err
	}
	err = dataBlock.PreKey.SetValue(preKeyIndex)
	if err != nil {
		return "", err
	}
	dataBlockAsBytes, err := dataBlock.MarshalBinary()
	if err != nil {
		return "", err
	}
	lvDataBlock, err := lv.NewLV(dataBlockAsBytes)
	if err != nil {
		return "", err
	}

	lvDataBlockAsBytes, err := lvDataBlock.MarshalBinary()
	if err != nil {
		return "", err
	}

	encyptedLVDataBlockAsBytes, err := aes.EncryptAES(encryptKey, lvDataBlockAsBytes)
	if err != nil {
		return "", err
	}
	lvEncyptedLVDataBlockAsBytes, err := lv.NewLV(encyptedLVDataBlockAsBytes)
	if err != nil {
		return "", err
	}

	signature := ed25519.Sign(edSelfPrivateKey[:], encyptedLVDataBlockAsBytes)
	lvSignature, err := lv.NewLV(signature)
	if err != nil {
		return "", err
	}

	lvDataBlockEnvelope, err := lv.CombineLV(lvEncyptedLVDataBlockAsBytes, lvSignature)
	if err != nil {
		return "", err
	}

	r, err = lvDataBlockEnvelope.AsHexString()
	if err != nil {
		return "", err
	}
	return r, nil
}

var UNPACK_TTL = 5 * time.Minute

func UnpackLVPayload(preKeyIndex string, peerPublicKey []byte, decryptKey []byte, dataAsHexString string) (r []*lv.LV, err error) {
	dataAsBytes, err := hex.DecodeString(dataAsHexString)
	if err != nil {
		return nil, err
	}

	lvData := lv.LV{}
	err = lvData.UnmarshalBinary(dataAsBytes)
	if err != nil {
		return nil, err
	}

	lvDataElements, err := lv.SeparateLV(&lvData)
	if err != nil {
		return nil, err
	}

	if lvDataElements == nil {
		return nil, errors.New("INVALID_DATA")
	}

	if len(lvDataElements) < 2 {
		return nil, errors.New("INVALID_DATA")
	}

	lvEncryptedData := lvDataElements[0]
	lvSignature := lvDataElements[1]

	valid := ed25519.Verify(peerPublicKey, lvEncryptedData.Value, lvSignature.Value)
	if !valid {
		return nil, errors.New(`INVALID_SIGNATURE`)
	}

	decryptedData, err := aes.DecryptAES(decryptKey, lvEncryptedData.Value)
	if err != nil {
		return nil, err
	}

	lvDecryptedLVDataBlock := lv.LV{}
	err = lvDecryptedLVDataBlock.UnmarshalBinary(decryptedData)
	if err != nil {
		return nil, err
	}

	dataBlock := DataBlock{}
	err = dataBlock.UnmarshalBinary(lvDecryptedLVDataBlock.Value)
	if err != nil {
		return nil, err
	}

	timeUnixNano := utils.BytesToInt64(dataBlock.Time.Value)
	dataBlockTime := time.Unix(0, timeUnixNano)

	if time.Now().Sub(dataBlockTime) > UNPACK_TTL {
		//		return nil, errors.New(`TIME_EXPIRED`)
	}

	dataBlockPreKeyIndex := string(dataBlock.PreKey.Value)

	if dataBlockPreKeyIndex != preKeyIndex {
		return nil, errors.New(`INVALID_PREKEY`)
	}

	if dataBlock.CheckDataHash() == false {
		return nil, errors.New(`INVALID_DATA_HASH`)
	}

	lvCombinedPayloadAsBytes := dataBlock.Data.Value
	lvCombinedPayload := lv.LV{}
	err = lvCombinedPayload.UnmarshalBinary(lvCombinedPayloadAsBytes)
	if err != nil {
		return nil, err
	}
	lvPtrDataPayload, err := lv.SeparateLV(&lvCombinedPayload)
	if err != nil {
		return nil, err
	}

	return lvPtrDataPayload, nil

}
