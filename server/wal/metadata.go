// Copyright 2024 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import "github.com/pkg/errors"

// +--------------+-------------------+-------------+--------------+
// | Size(4Bytes) | PreviousCRC(4Bytes) | CRC(4Bytes) | Payload(...) |
// +--------------+-------------------+-------------+--------------+
// Size: 			Length of the payload data
// PreviousCRC: 	32bit hash computed over the previous payload using CRC.
// CRC:				32bit hash computed over the previous and the current payload using CRC. CRC(n) = CRC( DATAn, CRCn-1 )
// Payload: 		Byte stream as long as specified by the payload size.

const (
	SizeLen    = 4
	CrcLen     = 4
	HeaderSize = SizeLen + CrcLen + CrcLen
)

type FormatVersion int

const TxnFormatVersion1 FormatVersion = 1
const TxnFormatVersion2 FormatVersion = 2

const TxnExtension = ".txn"
const TxnExtensionV2 = ".txnx"
const TdxExtension = ".idx"

var (
	ErrWalDataCorrupted = errors.New("WAL data corrupted")
)
