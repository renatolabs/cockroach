// Copyright 2022 The Cockroach Authors.
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
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

type upgradeFuncs struct{}

var upgrades upgradeFuncs

func (upgradeFuncs) upgradeNodes(
	ctx context.Context,
	nodes option.NodeListOption,
	startOpts option.StartOpts,
	newVersion string,
	t test.Test,
	c cluster.Cluster,
	l *logger.Logger,
) {
	// NB: We could technically stage the binary on all nodes before
	// restarting each one, but on Unix it's invalid to write to an
	// executable file while it is currently running. So we do the
	// simple thing and upload it serially instead.

	// Restart nodes in a random order; otherwise node 1 would be running all
	// the migrations and it probably also has all the leases.
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	for _, node := range nodes {
		v := newVersion
		if v == "" {
			v = "<latest>"
		}
		newVersionMsg := newVersion
		if newVersion == "" {
			newVersionMsg = "<current>"
		}
		l.Printf("restarting node %d into version %s", node, newVersionMsg)
		// Stop the cockroach process gracefully in order to drain it properly.
		// This makes the upgrade closer to how users do it in production, but
		// it's also needed to eliminate flakiness. In particular, this will
		// make sure that DistSQL draining information is dissipated through
		// gossip so that other nodes running an older version don't consider
		// this upgraded node for DistSQL plans (see #87154 for more details).
		// TODO(yuzefovich): ideally, we would also check that the drain was
		// successful since if it wasn't, then we might see flakes too.
		if err := c.StopCockroachGracefullyOnNode(ctx, l, node); err != nil {
			t.Fatal(err)
		}

		binary := uploadVersion(ctx, t, c, c.Node(node), newVersion)
		settings := install.MakeClusterSettings(install.BinaryOption(binary))
		c.Start(ctx, l, startOpts, settings, c.Node(node))

		// We have seen cases where a transient error could occur when this
		// newly upgraded node serves as a gateway for a distributed query due
		// to remote nodes not being able to dial back to the gateway for some
		// reason (investigation of it is tracked in #87634). For now, we're
		// papering over these flakes by this sleep. For more context, see
		// #87104.
		// TODO(yuzefovich): remove this sleep once #87634 is fixed.
		time.Sleep(4 * time.Second)
	}
}
