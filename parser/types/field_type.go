// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package types

// UnspecifiedLength is unspecified length.
const (
	UnspecifiedLength = -1
)

// FieldType records field type information.
type FieldType struct {
	Tp      byte
	Flag    uint
	Flen    int
	Decimal int
	Charset string
	Collate string
	// Elems is the element list for enum and set type.
	Elems []string
}

// NewFieldType returns a FieldType,
// with a type and other information about field type.
func NewFieldType(tp byte) *FieldType {
	return &FieldType{
		Tp:      tp,
		Flen:    UnspecifiedLength,
		Decimal: UnspecifiedLength,
	}
}

// Clone returns a copy of itself.
func (ft *FieldType) Clone() *FieldType {
	ret := *ft
	return &ret
}

// Init initializes the FieldType data.
func (ft *FieldType) Init(tp byte) {
	ft.Tp = tp
	ft.Flen = UnspecifiedLength
	ft.Decimal = UnspecifiedLength
}
