/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"reflect"
	"testing"
)

func TestParseControllerSelection(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    map[string]bool
		wantErr bool
	}{
		{
			name:  "all controllers",
			value: "all",
			want: map[string]bool{
				controllerCPGIngester:    true,
				controllerCarePlanWriter: true,
				controllerSandboxRequest: true,
			},
		},
		{
			name:  "sandbox controller only",
			value: controllerSandboxRequest,
			want:  map[string]bool{controllerSandboxRequest: true},
		},
		{
			name:  "selected pipeline controllers",
			value: controllerCPGIngester + ", " + controllerCarePlanWriter,
			want: map[string]bool{
				controllerCPGIngester:    true,
				controllerCarePlanWriter: true,
			},
		},
		{name: "unknown controller", value: "unknown", wantErr: true},
		{name: "all mixed with selection", value: "all,sandboxrequest", wantErr: true},
		{name: "empty selection", value: "", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseControllerSelection(test.value)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parse controller selection: %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("selection = %#v, want %#v", got, test.want)
			}
		})
	}
}
