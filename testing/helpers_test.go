package testing

import (
	"errors"
	"testing"
)

func TestTestInstance(t *testing.T) {
	instance := TestInstance(t)
	if instance == nil {
		t.Fatal("Expected instance, got nil")
	}
}

func TestAssertNoError_NoError(t *testing.T) {
	mockT := &testing.T{}
	AssertNoError(mockT, nil)
	if mockT.Failed() {
		t.Error("AssertNoError should not fail for nil error")
	}
}

func TestAssertError_WithError(t *testing.T) {
	mockT := &testing.T{}
	AssertError(mockT, errors.New("test error"))
	if mockT.Failed() {
		t.Error("AssertError should not fail when error is present")
	}
}

func TestAssertParams_Match(t *testing.T) {
	mockT := &testing.T{}
	AssertParams(mockT, []string{"a", "b"}, []string{"a", "b"})
	if mockT.Failed() {
		t.Error("AssertParams should not fail for matching params")
	}
}

func TestAssertContainsParam_Found(t *testing.T) {
	mockT := &testing.T{}
	AssertContainsParam(mockT, []string{"a", "b", "c"}, "b")
	if mockT.Failed() {
		t.Error("AssertContainsParam should not fail when param is found")
	}
}

func TestAssertPanics_WithPanic(t *testing.T) {
	mockT := &testing.T{}
	AssertPanics(mockT, func() {
		panic("test panic")
	})
	if mockT.Failed() {
		t.Error("AssertPanics should not fail when function panics")
	}
}

func TestContainsString(t *testing.T) {
	tests := []struct {
		s, substr string
		expected  bool
	}{
		{"hello world", "world", true},
		{"hello world", "hello", true},
		{"hello world", "xyz", false},
		{"", "", true},
		{"hello", "", true},
		{"", "hello", false},
	}

	for _, tt := range tests {
		result := containsString(tt.s, tt.substr)
		if result != tt.expected {
			t.Errorf("containsString(%q, %q) = %v, expected %v", tt.s, tt.substr, result, tt.expected)
		}
	}
}
