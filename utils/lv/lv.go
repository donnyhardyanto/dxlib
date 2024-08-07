package lv

import (
	"bytes"
	"dxlib/v3/utils"
	"encoding/binary"
	"encoding/hex"
	"errors"
)

var MAX_SIZE uint32 = 2147483647

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

func CombineLV(data ...*LV) (*LV, error) {
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

func SeparateLV(data *LV) ([]*LV, error) {
	r := bytes.NewReader(data.Value)
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

func (lv *LV) len() int32 {
	return int32(4 + len(lv.Value))
}

func (lv *LV) SetValue(data any) error {
	d, err := utils.AnyToBytes(data)
	if err != nil {
		return err
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
		return err
	}
	return nil
}

func (lv *LV) UnmarshalBinaryFromReader(r *bytes.Reader) error {
	err := binary.Read(r, binary.BigEndian, &lv.Length)
	if err != nil {
		return err
	}
	if lv.Length >= MAX_SIZE {
		return errors.New("LV.UnmarshalBinaryFromReader:ARRAY_SIZE_TOO_LARGE")
	}
	lv.Value = make([]byte, lv.Length)
	err = binary.Read(r, binary.BigEndian, &lv.Value)
	if err != nil {
		return err
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
