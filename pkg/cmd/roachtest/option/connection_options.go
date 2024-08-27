// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package option

import (
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

type CustomOption struct {
	name  string
	value any
}

func User(user string) CustomOption {
	return CustomOption{"User", user}
}

func VirtualClusterName(name string) CustomOption {
	return CustomOption{"VirtualClusterName", name}
}

func SQLInstance(sqlInstance int) CustomOption {
	return CustomOption{"SQLInstance", sqlInstance}
}

func ConnectionOption(key, value string) CustomOption {
	return CustomOption{"ConnectionOption", map[string]string{key: value}}
}

func ConnectTimeout(t time.Duration) CustomOption {
	sec := int64(t.Seconds())
	if sec < 1 {
		sec = 1
	}
	return CustomOption{"ConnectTimeout", sec}
}

func DBName(dbName string) CustomOption {
	return CustomOption{"DBName", dbName}
}

func AuthMode(authMode install.PGAuthMode) CustomOption {
	return CustomOption{"AuthMode", authMode}
}

func Apply(container any, opts []CustomOption) (retErr error) {
	s := reflect.ValueOf(container)

	var currentField string
	defer func() {
		if r := recover(); r != nil {
			if currentField == "" {
				retErr = fmt.Errorf("option.Apply failed: %v", r)
			} else {
				retErr = fmt.Errorf("failed to set %q on %T: %v", currentField, container, r)
			}
		}
	}()

	for _, opt := range opts {
		currentField = opt.name
		f := s.Elem().FieldByName(currentField)
		if !f.IsValid() {
			return fmt.Errorf("invalid option %s for %T", opt.name, container)
		}

		f.Set(reflect.ValueOf(opt.value))
	}

	return nil
}

type VirtualClusterOptions struct {
	VirtualClusterName string
	SQLInstance        int
}

type ConnOptions struct {
	VirtualClusterOptions
	User     string
	DBName   string
	AuthMode install.PGAuthMode
	Options  map[string]string
}
