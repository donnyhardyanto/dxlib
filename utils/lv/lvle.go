package lv

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// LVLE is the little-endian variant of LV.
// Wire format: [ uint32 little-endian length ][ value bytes ]
// Compatible with Python's bytes_vTol32v / api-proxy-v4 L32V encoding.
type LVLE struct {
	Length uint32
	Value  []byte
}

func NewLVLE(data []byte) (*LVLE, error) {
	b := &LVLE{}
	if err := b.SetValue(data); err != nil {
		return nil, err
	}
	return b, nil
}

func NewLVLEFromBinary(data []byte) (*LVLE, error) {
	b := &LVLE{}
	if err := b.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return b, nil
}

func CombineLVLE(data ...*LVLE) (*LVLE, error) {
	return CombineLVLEs(data)
}

func CombineLVLEs(data []*LVLE) (*LVLE, error) {
	buf := new(bytes.Buffer)
	for _, v := range data {
		b, err := v.MarshalBinary()
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, b); err != nil {
			return nil, err
		}
	}
	return NewLVLE(buf.Bytes())
}

func (lv *LVLE) Expand() ([]*LVLE, error) {
	r := bytes.NewReader(lv.Value)
	var lvs []*LVLE
	for r.Len() > 0 {
		child := &LVLE{}
		if err := child.UnmarshalBinaryFromReader(r); err != nil {
			return nil, err
		}
		lvs = append(lvs, child)
	}
	return lvs, nil
}

func (lv *LVLE) SetValue(data any) error {
	d, err := utils.AnyToBytes(data)
	if err != nil {
		return errors.Wrap(err, "ERROR_IN_LVLE_SET_VALUE_ANY_TO_BYTES")
	}
	lv.Value = d
	lv.Length = uint32(len(d))
	return nil
}

func (lv *LVLE) GetValueAsString() string {
	return string(lv.Value)
}

func (lv *LVLE) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, lv.Length); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, lv.Value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (lv *LVLE) UnmarshalBinary(data []byte) error {
	buf := bytes.NewReader(data)
	if err := lv.UnmarshalBinaryFromReader(buf); err != nil {
		return errors.Wrap(err, "ERROR_IN_LVLE_UNMARSHAL_BINARY_FROM_READER")
	}
	return nil
}

func (lv *LVLE) UnmarshalBinaryFromReader(r *bytes.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &lv.Length); err != nil {
		return errors.Wrap(err, "ERROR_IN_LVLE_UNMARSHAL_BINARY_FROM_READER_BINARY_READ_LENGTH")
	}
	lv.Value = make([]byte, lv.Length)
	if err := binary.Read(r, binary.LittleEndian, &lv.Value); err != nil {
		return errors.Wrap(err, "ERROR_IN_LVLE_UNMARSHAL_BINARY_FROM_READER_BINARY_READ_VALUE")
	}
	return nil
}

func (lv *LVLE) AsHexString() (string, error) {
	b, err := lv.MarshalBinary()
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
