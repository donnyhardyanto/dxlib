package lv

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"

	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/donnyhardyanto/dxlib/errors"
)

//var MAX_SIZE uint32 = 2147483647

type LV struct {
	Length uint32
	Value  []byte
}

func NewLV(data []byte) (*LV, error) {
	b := &LV{}
	err := b.SetValue(data)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func NewLVFromBinary(data []byte) (*LV, error) {
	b := &LV{}
	err := b.UnmarshalBinary(data)
	if err != nil {
		return nil, err
	}
	return b, nil
}
func CombineLV(data ...*LV) (*LV, error) {
	return CombineLVs(data)
}

func CombineLVs(data []*LV) (*LV, error) {
	buf := new(bytes.Buffer)
	for _, v := range data {
		b, err := v.MarshalBinary()
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, b)
		if err != nil {
			return nil, err
		}
	}
	lv, err := NewLV(buf.Bytes())
	if err != nil {
		return nil, err
	}
	return lv, nil
}

func (lv *LV) Expand() ([]*LV, error) {
	r := bytes.NewReader(lv.Value)
	var lvs []*LV
	for r.Len() > 0 {
		lv := &LV{}
		err := lv.UnmarshalBinaryFromReader(r)
		if err != nil {
			return nil, err
		}
		lvs = append(lvs, lv)
	}
	return lvs, nil
}

func (lv *LV) SetValue(data any) error {
	d, err := utils.AnyToBytes(data)
	if err != nil {
		return errors.Wrap(err, "ERROR_IN_LV_SET_VALUE_ANY_TO_BYTES")
	}
	lv.Value = d
	lv.Length = uint32(len(d))
	return nil
}

func (lv *LV) GetValueAsString() string {
	return string(lv.Value)
}

func (lv *LV) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, lv.Length)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.BigEndian, lv.Value)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (lv *LV) UnmarshalBinary(data []byte) error {
	buf := bytes.NewReader(data)
	err := lv.UnmarshalBinaryFromReader(buf)
	if err != nil {
		return errors.Wrap(err, "ERROR_IN_LV_UNMARSHAL_BINARY_FROM_READER")
	}
	return nil
}

func (lv *LV) UnmarshalBinaryFromReader(r *bytes.Reader) error {
	err := binary.Read(r, binary.BigEndian, &lv.Length)
	if err != nil {
		return errors.Wrap(err, "ERROR_IN_LV_UNMARSHAL_BINARY_FROM_READER_BINARY_READ_LENGTH")
	}

	lv.Value = make([]byte, lv.Length)
	err = binary.Read(r, binary.BigEndian, &lv.Value)
	if err != nil {
		return errors.Wrap(err, "ERROR_IN_LV_UNMARSHAL_BINARY_FROM_READER_BINARY_READ_VALUE")
	}
	return nil
}

func (lv *LV) AsHexString() (r string, err error) {
	b, err := lv.MarshalBinary()
	if err != nil {
		return "", err
	}
	r = hex.EncodeToString(b)
	return r, nil
}
