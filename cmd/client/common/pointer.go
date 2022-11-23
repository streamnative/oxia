package common

func PtrBool(b bool) *bool {
	return &b
}

func PtrInt64(i int64) *int64 {
	return &i
}

func PtrString(s string) *string {
	return &s
}
