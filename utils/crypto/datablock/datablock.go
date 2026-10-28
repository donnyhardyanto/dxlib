package datablock

import (
	"bytes"
	stdaes "crypto/aes"
	"crypto/ed25519"
	"crypto/sha512"
	"encoding/hex"
	"time"
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils/crypto/aes"
	"github.com/donnyhardyanto/dxlib/utils/crypto/rand"
	"github.com/donnyhardyanto/dxlib/utils/lv"
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
		return errors.Wrap(err, "ERROR_IN_DATA_BLOCK_SET_TIME_NOW")
	}
	return nil
}

func (db *DataBlock) GenerateNonce() (err error) {
	err = db.Nonce.SetValue(rand.RandomData(32))
	if err != nil {
		return errors.Wrap(err, "ERROR_IN_DATA_BLOCK_GENERATE_NONCE")
	}
	return nil
}

func (db *DataBlock) SetDataValue(data any) (err error) {
	err = db.Data.SetValue(data)
	if err != nil {
		return errors.Wrap(err, "ERROR_IN_DATA_BLOCK_SET_DATA_VALUE")
	}
	err = db.GenerateDataHash()
	if err != nil {
		return errors.Wrap(err, "ERROR_IN_DATA_BLOCK_SET_DATA_VALUE_GENERATE_DATA_HASH")
	}
	return nil
}

func (db *DataBlock) GenerateDataHash() (err error) {
	dataAsBytes := db.Data.Value
	x := sha512.Sum512(dataAsBytes)
	err = db.DataHash.SetValue(x[:])
	if err != nil {
		return errors.Wrap(err, "ERROR_IN_DATA_BLOCK_GENERATE_DATA_HASH")
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

// PackLVPayload packs payloads into a signed, encrypted envelope: the data
// block (time, nonce, pre-key index, the combined payloads and their SHA-512)
// is encrypted with aes.EncryptAES under encryptKey, signed with
// edSelfPrivateKey, and the two are combined as LVs and hex-encoded.
//
// The envelope is written in the format every reader so far understands,
// so a mixed set of peers keeps working. PackLVPayloadAESGCM writes the
// authenticated format; UnpackLVPayload reads both.
func PackLVPayload(preKeyIndex string, edSelfPrivateKey []byte, encryptKey []byte, payloads ...*lv.LV) (r string, err error) {
	return packLVPayload(preKeyIndex, edSelfPrivateKey, func(data []byte) ([]byte, error) {
		return aes.EncryptAES(encryptKey, data)
	}, payloads...)
}

// PackLVPayloadAESGCM is PackLVPayload with the data block encrypted by
// aes.EncryptAESGCM (encryptKey is 32 bytes; no associated data). Only a
// reader with UnpackLVPayload from this version on can open it, so use it
// once every peer that reads the envelope has moved.
func PackLVPayloadAESGCM(preKeyIndex string, edSelfPrivateKey []byte, encryptKey []byte, payloads ...*lv.LV) (r string, err error) {
	return packLVPayload(preKeyIndex, edSelfPrivateKey, func(data []byte) ([]byte, error) {
		return aes.EncryptAESGCM(encryptKey, data, nil)
	}, payloads...)
}

func packLVPayload(preKeyIndex string, edSelfPrivateKey []byte, encrypt func(data []byte) ([]byte, error), payloads ...*lv.LV) (r string, err error) {
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

	encyptedLVDataBlockAsBytes, err := encrypt(lvDataBlockAsBytes)
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

// UnpackLVPayload opens an envelope from PackLVPayload or PackLVPayloadAESGCM:
// it checks the Ed25519 signature with peerPublicKey, decrypts the data block
// with decryptKey in whichever format it was written, and checks its age
// (PayloadUnpackTTL), pre-key index and hash before returning the payloads.
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
		return nil, errors.New("INVALID_SIGNATURE")
	}

	decryptedData, err := decryptDataBlock(decryptKey, lvEncryptedData.Value)
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
		return nil, errors.New("TIME_EXPIRED")
	}

	dataBlockPreKeyIndex := string(dataBlock.PreKey.Value)

	if dataBlockPreKeyIndex != preKeyIndex {
		return nil, errors.New("INVALID_PREKEY")
	}

	if dataBlock.CheckDataHash() == false {
		return nil, errors.New("INVALID_DATA_HASH")
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

// decryptDataBlock reads the data block in either format. The signature
// over the ciphertext has been checked by the caller, so the bytes are the
// peer's own; what is decided here is only which helper wrote them. A block
// with the shape aes.EncryptAESGCM writes is opened with aes.DecryptAESGCM;
// when that fails and the block also has the shape aes.EncryptAES writes
// (an IV and whole blocks), it is a CBC block whose random IV happens to
// start with the format byte, and aes.DecryptAES reads it. A block with
// neither shape is refused here, so no input length reaches the CBC helper.
func decryptDataBlock(key, data []byte) ([]byte, error) {
	if aes.IsAESGCMFormat(data) {
		plaintext, err := aes.DecryptAESGCM(key, data, nil)
		if err == nil {
			return plaintext, nil
		}
		if !isCBCShaped(data) {
			return nil, err
		}
	}
	if !isCBCShaped(data) {
		return nil, errors.Errorf("INVALID_DATA: the encrypted block is %d bytes, which fits neither format (the authenticated one needs %d bytes or more with the format byte first, the other an IV and whole blocks of %d bytes)", len(data), aes.AESGCMOverhead, stdaes.BlockSize)
	}
	return aes.DecryptAES(key, data)
}

// isCBCShaped reports whether data could be aes.EncryptAES output: a
// 16-byte IV followed by at least one whole block.
func isCBCShaped(data []byte) bool {
	return len(data) >= 2*stdaes.BlockSize && len(data)%stdaes.BlockSize == 0
}
