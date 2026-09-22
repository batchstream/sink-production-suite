package testevents

import (
	"strings"
	"testing"
)

func TestRejectsFalseGreenQualification(t *testing.T) {
	start := `{"Action":"start","Package":"suite"}` + "\n"
	run := `{"Action":"run","Package":"suite","Test":"TestContract"}` + "\n"
	pass := `{"Action":"pass","Package":"suite","Test":"TestContract"}` + "\n"
	finish := `{"Action":"pass","Package":"suite"}` + "\n"
	valid := start + run + pass + finish
	cases := []struct {
		name      string
		data      string
		required  []string
		wantError bool
	}{
		{name: "valid", data: valid, required: []string{"TestContract"}},
		{name: "empty", wantError: true},
		{name: "package only", data: start + finish, wantError: true},
		{name: "skip", data: start + run + `{"Action":"skip","Test":"TestContract"}` + finish, wantError: true},
		{name: "failure", data: valid + `{"Action":"fail","Package":"other"}`, wantError: true},
		{name: "unfinished package", data: start + run + pass, wantError: true},
		{name: "unfinished test", data: start + run + finish, wantError: true},
		{name: "missing contract", data: valid, required: []string{"TestIncident"}, wantError: true},
		{name: "truncated json", data: valid + `{`, wantError: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Check(strings.NewReader(tc.data), tc.required)
			if (err != nil) != tc.wantError {
				t.Fatalf("Check() = %v, wantError=%t", err, tc.wantError)
			}
		})
	}
}
