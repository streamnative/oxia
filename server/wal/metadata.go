package wal

const (
	CrcLen         = 4
	PayloadSizeLen = 4
	HeaderLen      = 8
)

type FormatVersion int

const TxnFormatVersion1 FormatVersion = 1
const TxnFormatVersion2 FormatVersion = 2
