package datablock

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha512"
	"encoding/hex"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/donnyhardyanto/dxlib/utils/crypto/aes"
	"github.com/donnyhardyanto/dxlib/utils/lv"
	"github.com/pkg/errors"
	"time"
	_ "time/tzdata"
)

var PayloadUnpackTTL = 5 * time.Minute

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

func NewDataBlockFromLV(aLV *lv.LV) (*DataBlock, error) {
	lvs, err := aLV.Expand()
	if err != nil {
		return nil, err
	}
	b := &DataBlock{
		Time:     *lvs[0],
		Nonce:    *lvs[1],
		PreKey:   *lvs[2],
		Data:     *lvs[3],
		DataHash: *lvs[4],
	}
	return b, nil
}
func (db *DataBlock) SetTimeNow() error {
	t := time.Now().UTC()
	tAsString := t.Format(time.RFC3339)
	tAsBytes := []byte(tAsString)
	err := db.Time.SetValue(tAsBytes)
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

func (db *DataBlock) AsLV() (r *lv.LV, err error) {
	r, err = lv.CombineLV(&db.Time, &db.Nonce, &db.PreKey, &db.Data, &db.DataHash)
	if err != nil {
		return nil, err
	}
	return r, nil
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

	lvDataBlock, err := dataBlock.AsLV()
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

	lvDataElements, err := lvData.Expand()
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

	lvDecryptedLVDataBlock, err := lv.NewLVFromBinary(decryptedData)
	if err != nil {
		return nil, err
	}

	dataBlock, err := NewDataBlockFromLV(lvDecryptedLVDataBlock)
	if err != nil {
		return nil, err
	}

	timestamp := dataBlock.Time.GetValueAsString()
	parsedTimestamp, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return nil, err
	}

	if time.Now().Sub(parsedTimestamp) > PayloadUnpackTTL {
		return nil, errors.New(`TIME_EXPIRED`)
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
	lvPtrDataPayload, err := lvCombinedPayload.Expand()
	if err != nil {
		return nil, err
	}

	return lvPtrDataPayload, nil

}
