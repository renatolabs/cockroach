// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

func registerDrop(r registry.Registry) {
	// TODO(tschottdorf): rearrange all tests so that their synopses are available
	// via godoc and (some variation on) `roachtest run <testname> --help`.

	// This test imports a TPCC dataset and then issues a manual deletion followed
	// by a truncation for the `stock` table (which contains warehouses*100k
	// rows). Next, it issues a `DROP` for the whole database, and sets the GC TTL
	// to one second.
	runDrop := func(ctx context.Context, t test.Test, c cluster.Cluster, warehouses, nodes int) {
		c.Put(ctx, t.Cockroach(), "./cockroach", c.Range(1, nodes))
		c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Range(1, nodes))
		settings := install.MakeClusterSettings()
		c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Range(1, nodes))

		m := c.NewMonitor(ctx, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			time.Sleep(10 * time.Second)
			t.Fatal("simulating failure")

			return nil
		})
		m.Go(func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return fmt.Errorf("worker: context canceled")
				default:
					t.L().Printf("doing work...")
					time.Sleep(1 * time.Second)
				}
			}
		})
		m.Wait()
	}

	warehouses := 100
	numNodes := 2

	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("drop/tpcc/w=%d,nodes=%d", warehouses, numNodes),
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(numNodes),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runDrop(ctx, t, c, warehouses, numNodes)
		},
	})
}
