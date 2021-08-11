// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package encoding

import (
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

type uint64TestCase struct {
	decoded uint64
	encoded []byte
}

var varuint64TestCases = []uint64TestCase{
	{0, []byte{0x00}},
	{1, []byte{0x01}},
	{127, []byte{0x7F}},
	{128, []byte{0x80, 0x01}},
	{129, []byte{0x81, 0x01}},
	{255, []byte{0xFF, 0x01}},
	{256, []byte{0x80, 0x02}},
	{16383, []byte{0xFF, 0x7F}},
	{16384, []byte{0x80, 0x80, 0x01}},
	{16385, []byte{0x81, 0x80, 0x01}},
	{math.MaxUint64 - 1, []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
	{math.MaxUint64, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
}

func TestEncodeVaruint64(t *testing.T) {
	for _, testCase := range varuint64TestCases {
		encoded := []byte{}
		EncodeUvarint64(&encoded, testCase.decoded)
		assert.Equal(t, testCase.encoded, encoded)
	}
}

func TestDecodeVaruint64(t *testing.T) {
	for _, testCase := range varuint64TestCases {
		enc := testCase.encoded
		decoded, err := DecodeUvarint64(&enc)
		assert.Equal(t, testCase.decoded, decoded)
		assert.Nil(t, err)
		assert.Zero(t, len(enc))
	}
	{
		_, err := DecodeUvarint64(&[]byte{})
		assert.Equal(t, err, io.EOF)
	}
	{
		_, err := DecodeUvarint64(&[]byte{0x80})
		assert.Equal(t, err, io.EOF)
	}
}

func TestUvaruint64Size(t *testing.T) {
	for _, testCase := range varuint64TestCases {
		assert.Equal(t, len(testCase.encoded), Uvarint64Size(testCase.decoded))
	}
}

type int64TestCase struct {
	decoded int64
	encoded []byte
}

var varint64TestCases = []int64TestCase{
	{0, []byte{0x00}},
	{1, []byte{0x02}},
	{63, []byte{0x7E}},
	{64, []byte{0x80, 0x01}},
	{65, []byte{0x82, 0x01}},
	{127, []byte{0xFE, 0x01}},
	{128, []byte{0x80, 0x02}},
	{8191, []byte{0xFE, 0x7F}},
	{8192, []byte{0x80, 0x80, 0x01}},
	{8193, []byte{0x82, 0x80, 0x01}},
	{math.MaxInt64>>1 - 1, []byte{0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
	{math.MaxInt64 >> 1, []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
	{math.MaxInt64>>1 + 1, []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}},
	{math.MaxInt64 - 1, []byte{0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
	{math.MaxInt64, []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
	{-1, []byte{0x01}},
	{-63, []byte{0x7D}},
	{-64, []byte{0x7F}},
	{-65, []byte{0x81, 0x01}},
	{-127, []byte{0xFD, 0x01}},
	{-128, []byte{0xFF, 0x01}},
	{-8191, []byte{0xFD, 0x7F}},
	{-8192, []byte{0xFF, 0x7F}},
	{-8193, []byte{0x81, 0x80, 0x01}},
	{math.MinInt64>>1 + 1, []byte{0xFD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
	{math.MinInt64 >> 1, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
	{math.MinInt64>>1 - 1, []byte{0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}},
	{math.MinInt64 + 1, []byte{0xFD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
	{math.MinInt64, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
}

func TestEncodeVarint64(t *testing.T) {
	for _, testCase := range varint64TestCases {
		encoded := []byte{}
		EncodeVarint64(&encoded, testCase.decoded)
		assert.Equal(t, testCase.encoded, encoded)
	}
}

func TestDecodeVarint64(t *testing.T) {
	for _, testCase := range varint64TestCases {
		enc := testCase.encoded
		decoded, err := DecodeVarint64(&enc)
		assert.Equal(t, testCase.decoded, decoded)
		assert.Nil(t, err)
		assert.Zero(t, len(enc))
	}
	{
		_, err := DecodeVarint32(&[]byte{})
		assert.Equal(t, err, io.EOF)
	}
	{
		_, err := DecodeVarint32(&[]byte{0x80})
		assert.Equal(t, err, io.EOF)
	}
	{
		_, err := DecodeVarint32(&[]byte{0x80, 0x80, 0x80, 0x80, 0x10})
		assert.Equal(t, err, errVarint32Overflow)
	}
	{
		_, err := DecodeVarint32(&[]byte{0x81, 0x80, 0x80, 0x80, 0x10})
		assert.Equal(t, err, errVarint32Overflow)
	}
}

func TestVaruint64Size(t *testing.T) {
	for _, testCase := range varint64TestCases {
		assert.Equal(t, len(testCase.encoded), Varint64Size(testCase.decoded))
	}
}

type int32TestCase struct {
	decoded int32
	encoded []byte
}

var varint32TestCases = []int32TestCase{
	{0, []byte{0x00}},
	{1, []byte{0x02}},
	{63, []byte{0x7E}},
	{64, []byte{0x80, 0x01}},
	{65, []byte{0x82, 0x01}},
	{127, []byte{0xFE, 0x01}},
	{128, []byte{0x80, 0x02}},
	{8191, []byte{0xFE, 0x7F}},
	{8192, []byte{0x80, 0x80, 0x01}},
	{8193, []byte{0x82, 0x80, 0x01}},
	{math.MaxInt32>>1 - 1, []byte{0xFC, 0xFF, 0xFF, 0xFF, 0x07}},
	{math.MaxInt32 >> 1, []byte{0xFE, 0xFF, 0xFF, 0xFF, 0x07}},
	{math.MaxInt32>>1 + 1, []byte{0x80, 0x80, 0x80, 0x80, 0x08}},
	{math.MaxInt32 - 1, []byte{0xFC, 0xFF, 0xFF, 0xFF, 0x0F}},
	{math.MaxInt32, []byte{0xFE, 0xFF, 0xFF, 0xFF, 0x0F}},
	{-1, []byte{0x01}},
	{-63, []byte{0x7D}},
	{-64, []byte{0x7F}},
	{-65, []byte{0x81, 0x01}},
	{-127, []byte{0xFD, 0x01}},
	{-128, []byte{0xFF, 0x01}},
	{-8191, []byte{0xFD, 0x7F}},
	{-8192, []byte{0xFF, 0x7F}},
	{-8193, []byte{0x81, 0x80, 0x01}},
	{math.MinInt32>>1 + 1, []byte{0xFD, 0xFF, 0xFF, 0xFF, 0x07}},
	{math.MinInt32 >> 1, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x07}},
	{math.MinInt32>>1 - 1, []byte{0x81, 0x80, 0x80, 0x80, 0x08}},
	{math.MinInt32 + 1, []byte{0xFD, 0xFF, 0xFF, 0xFF, 0x0F}},
	{math.MinInt32, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}},
}

func TestEncodeVarint32(t *testing.T) {
	for _, testCase := range varint32TestCases {
		encoded := []byte{}
		EncodeVarint64(&encoded, int64(testCase.decoded))
		assert.Equal(t, testCase.encoded, encoded)
	}
}

func TestDecodeVarint32(t *testing.T) {
	for _, testCase := range varint32TestCases {
		enc := testCase.encoded
		decoded, err := DecodeVarint32(&enc)
		assert.Equal(t, testCase.decoded, decoded)
		assert.Nil(t, err)
		assert.Zero(t, len(enc))
	}
	{
		_, err := DecodeVarint32(&[]byte{})
		assert.Equal(t, err, io.EOF)
	}
	{
		_, err := DecodeVarint32(&[]byte{0x80})
		assert.Equal(t, err, io.EOF)
	}
	{
		_, err := DecodeVarint32(&[]byte{0x80, 0x80, 0x80, 0x80, 0x10})
		assert.Equal(t, err, errVarint32Overflow)
	}
	{
		_, err := DecodeVarint32(&[]byte{0x81, 0x80, 0x80, 0x80, 0x10})
		assert.Equal(t, err, errVarint32Overflow)
	}
}

type float64TestCase struct {
	decoded float64
	encoded []byte
}

var float64LETestCases = []float64TestCase{
	{0, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	{1, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F}},
	{-2, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0}},
}

func TestEncodeFloat64LE(t *testing.T) {
	for _, testCase := range float64LETestCases {
		encoded := []byte{}
		EncodeFloat64LE(&encoded, testCase.decoded)
		assert.Equal(t, testCase.encoded, encoded)
	}
}

func TestDecodeFloat64LE(t *testing.T) {
	for _, testCase := range float64LETestCases {
		enc := testCase.encoded
		decoded, err := DecodeFloat64LE(&enc)
		assert.Equal(t, testCase.decoded, decoded)
		assert.Nil(t, err)
		assert.Zero(t, len(enc))
	}
	{
		_, err := DecodeFloat64LE(&[]byte{})
		assert.Equal(t, err, io.EOF)
	}
	{
		_, err := DecodeFloat64LE(&[]byte{0x00})
		assert.Equal(t, err, io.EOF)
	}
}

var varfloat64TestCases = []float64TestCase{
	{0, []byte{0x00}},
	{1, []byte{0x02}},
	{2, []byte{0x03}},
	{3, []byte{0x04}},
	{4, []byte{0x84, 0x40}},
	{5, []byte{0x05}},
	{6, []byte{0x85, 0x40}},
	{7, []byte{0x06}},
	{8, []byte{0x86, 0x20}},
	{9, []byte{0x86, 0x40}},
	{float64(uint64(1)<<52 - 2), []byte{0xE7, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x80}},
	{float64(uint64(1)<<52 - 1), []byte{0x68}},
	{float64(uint64(1) << 52), []byte{0xE8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x40}},
	{float64(uint64(1)<<53 - 2), []byte{0xE9, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xC0}},
	{float64(uint64(1)<<53 - 1), []byte{0x6A}},
	{-1, []byte{0x82, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x30}},
	{-0.5, []byte{0xFE, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x3F}},
}

func TestEncodeVarfloat64(t *testing.T) {
	for _, testCase := range varfloat64TestCases {
		encoded := []byte{}
		EncodeVarfloat64(&encoded, testCase.decoded)
		assert.Equal(t, testCase.encoded, encoded)
	}
}

func TestDecodeVarfloat64(t *testing.T) {
	for _, testCase := range varfloat64TestCases {
		enc := testCase.encoded
		decoded, err := DecodeVarfloat64(&enc)
		assert.Equal(t, testCase.decoded, decoded)
		assert.Nil(t, err)
		assert.Zero(t, len(enc))
	}
	{
		_, err := DecodeVarfloat64(&[]byte{})
		assert.Equal(t, err, io.EOF)
	}
	{
		_, err := DecodeVarfloat64(&[]byte{0x80})
		assert.Equal(t, err, io.EOF)
	}
}

func TestVarfloat64Size(t *testing.T) {
	for _, testCase := range varfloat64TestCases {
		assert.Equal(t, len(testCase.encoded), Varfloat64Size(testCase.decoded))
	}
}
