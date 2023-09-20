// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTenantLabel(t *testing.T) {
	testCases := []struct {
		name           string
		tenantName     string
		tenantInstance int
		expectedLabel  string
	}{
		{
			name:          "empty tenant name",
			tenantName:    "",
			expectedLabel: "cockroach-system",
		},
		{
			name:          "system tenant name",
			tenantName:    "system",
			expectedLabel: "cockroach-system",
		},
		{
			name:           "simple app tenant name",
			tenantName:     "a",
			tenantInstance: 1,
			expectedLabel:  "cockroach-a_1",
		},
		{
			name:           "tenant name with hyphens",
			tenantName:     "tenant-a-1",
			tenantInstance: 1,
			expectedLabel:  "cockroach-tenant-a-1_1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			label := TenantLabel(tc.tenantName, tc.tenantInstance)
			require.Equal(t, tc.expectedLabel, label)

			nameFromLabel, instanceFromLabel, err := TenantInfoFromLabel(label)
			require.NoError(t, err)

			expectedTenantName := tc.tenantName
			if tc.tenantName == "" {
				expectedTenantName = "system"
			}
			require.Equal(t, expectedTenantName, nameFromLabel)

			require.Equal(t, tc.tenantInstance, instanceFromLabel)
		})
	}
}
