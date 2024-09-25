package common

type WalOptions struct {
	Namespace string
	Shard     int64
	WalDir    string
}

var WalOption WalOptions
