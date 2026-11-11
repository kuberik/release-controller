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

package utils

import "testing"

func TestCheckKubeContext(t *testing.T) {
	cases := []struct {
		current, cluster string
		wantErr          bool
	}{
		{"kind-rc-e2e", "rc-e2e", false},
		{"kind-rollout-dev", "rc-e2e", true},
		{"", "rc-e2e", true},
		{"rc-e2e", "rc-e2e", true},
	}
	for _, c := range cases {
		err := CheckKubeContext(c.current, c.cluster)
		if (err != nil) != c.wantErr {
			t.Errorf("CheckKubeContext(%q, %q) error = %v, wantErr %v", c.current, c.cluster, err, c.wantErr)
		}
	}
}
