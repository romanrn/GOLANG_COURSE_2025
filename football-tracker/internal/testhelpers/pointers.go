package testhelpers

// IntPtr returns a pointer to an int value
func IntPtr(i int) *int {
	return &i
}

// StringPtr returns a pointer to a string value
func StringPtr(s string) *string {
	return &s
}

// BoolPtr returns a pointer to a bool value
func BoolPtr(b bool) *bool {
	return &b
}

// Int64Ptr returns a pointer to an int64 value
func Int64Ptr(i int64) *int64 {
	return &i
}

// Float64Ptr returns a pointer to a float64 value
func Float64Ptr(f float64) *float64 {
	return &f
}
