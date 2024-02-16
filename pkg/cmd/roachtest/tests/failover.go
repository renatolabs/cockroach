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
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/failers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

var rangeLeaseRenewalDuration = func() time.Duration {
	var raftCfg base.RaftConfig
	raftCfg.SetDefaults()
	return raftCfg.RangeLeaseRenewalDuration()
}()

// registerFailover registers a set of failover benchmarks. These tests
// benchmark the maximum unavailability experienced by clients during various
// node failures, and exports them for roachperf graphing. They do not make any
// assertions on recovery time, similarly to other performance benchmarks.
//
// The tests run a kv workload against a cluster while subjecting individual
// nodes to various failures. The workload uses dedicated SQL gateway nodes that
// don't fail, relying on these for error handling and retries. The pMax latency
// seen by any client is exported and graphed. Since recovery times are
// probabilistic, each test performs multiple failures (typically 9) in order to
// find the worst-case recovery time following a failure.
//
// Failures last for 60 seconds before the node is recovered. Thus, the maximum
// measured unavailability is 60 seconds, which in practice means permanent
// unavailability (for some or all clients).
//
// No attempt is made to find the distribution of recovery times (e.g. the
// minimum and median), since this requires more sophisticated data recording
// and analysis. Simply taking the median across all requests is not sufficient,
// since requests are also executed against a healthy cluster between failures,
// and against healthy replicas during failures, thus the vast majority of
// requests are successful with nominal latencies. See also:
// https://github.com/cockroachdb/cockroach/issues/103654
func registerFailover(r registry.Registry) {
	for _, leases := range []registry.LeaseType{registry.EpochLeases, registry.ExpirationLeases} {
		var suffix string
		if leases == registry.ExpirationLeases {
			suffix = "/lease=expiration"
		}

		for _, readOnly := range []bool{false, true} {
			readOnly := readOnly // pin loop variable
			suffix := suffix
			if readOnly {
				suffix = "/read-only" + suffix
			} else {
				suffix = "/read-write" + suffix
			}
			r.Add(registry.TestSpec{
				Name:                "failover/chaos" + suffix,
				Owner:               registry.OwnerKV,
				Benchmark:           true,
				Timeout:             60 * time.Minute,
				Cluster:             r.MakeClusterSpec(10, spec.CPU(2), spec.DisableLocalSSD(), spec.ReuseNone()), // uses disk stalls
				CompatibleClouds:    registry.AllExceptAWS,
				Suites:              registry.Suites(registry.Nightly),
				Leases:              leases,
				SkipPostValidations: registry.PostValidationNoDeadNodes, // cleanup kills nodes
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runFailoverChaos(ctx, t, c, readOnly)
				},
			})
		}

		r.Add(registry.TestSpec{
			Name:             "failover/partial/lease-gateway" + suffix,
			Owner:            registry.OwnerKV,
			Benchmark:        true,
			Timeout:          30 * time.Minute,
			Cluster:          r.MakeClusterSpec(8, spec.CPU(2)),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           leases,
			Run:              runFailoverPartialLeaseGateway,
		})

		r.Add(registry.TestSpec{
			Name:             "failover/partial/lease-leader" + suffix,
			Owner:            registry.OwnerKV,
			Benchmark:        true,
			Timeout:          30 * time.Minute,
			Cluster:          r.MakeClusterSpec(7, spec.CPU(2)),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           leases,
			Run:              runFailoverPartialLeaseLeader,
		})

		r.Add(registry.TestSpec{
			Name:             "failover/partial/lease-liveness" + suffix,
			Owner:            registry.OwnerKV,
			Benchmark:        true,
			Timeout:          30 * time.Minute,
			Cluster:          r.MakeClusterSpec(8, spec.CPU(2)),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           leases,
			Run:              runFailoverPartialLeaseLiveness,
		})

		for _, failureMode := range failers.AllFailureModes {
			failureMode := failureMode // pin loop variable

			clusterOpts := make([]spec.Option, 0)
			clusterOpts = append(clusterOpts, spec.CPU(2))

			var postValidation registry.PostValidation
			if failureMode == failers.FailureModeDiskStall {
				// Use PDs in an attempt to work around flakes encountered when using
				// SSDs. See #97968.
				clusterOpts = append(clusterOpts, spec.DisableLocalSSD())
				// Don't reuse the cluster for tests that call dmsetup to avoid
				// spurious flakes from previous runs. See #107865
				clusterOpts = append(clusterOpts, spec.ReuseNone())
				postValidation = registry.PostValidationNoDeadNodes
			}
			r.Add(registry.TestSpec{
				Name:                fmt.Sprintf("failover/non-system/%s%s", failureMode, suffix),
				Owner:               registry.OwnerKV,
				Benchmark:           true,
				Timeout:             30 * time.Minute,
				SkipPostValidations: postValidation,
				Cluster:             r.MakeClusterSpec(7, clusterOpts...),
				CompatibleClouds:    registry.AllExceptAWS,
				Suites:              registry.Suites(registry.Nightly),
				Leases:              leases,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runFailoverNonSystem(ctx, t, c, failureMode)
				},
			})
			r.Add(registry.TestSpec{
				Name:                fmt.Sprintf("failover/liveness/%s%s", failureMode, suffix),
				Owner:               registry.OwnerKV,
				CompatibleClouds:    registry.AllExceptAWS,
				Suites:              registry.Suites(registry.Weekly),
				Benchmark:           true,
				Timeout:             30 * time.Minute,
				SkipPostValidations: postValidation,
				Cluster:             r.MakeClusterSpec(5, clusterOpts...),
				Leases:              leases,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runFailoverLiveness(ctx, t, c, failureMode)
				},
			})
			r.Add(registry.TestSpec{
				Name:                fmt.Sprintf("failover/system-non-liveness/%s%s", failureMode, suffix),
				Owner:               registry.OwnerKV,
				CompatibleClouds:    registry.AllExceptAWS,
				Suites:              registry.Suites(registry.Weekly),
				Benchmark:           true,
				Timeout:             30 * time.Minute,
				SkipPostValidations: postValidation,
				Cluster:             r.MakeClusterSpec(7, clusterOpts...),
				Leases:              leases,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runFailoverSystemNonLiveness(ctx, t, c, failureMode)
				},
			})
		}
	}
}

// runFailoverChaos sets up a 9-node cluster with RF=5 and randomly scattered
// ranges and replicas, and then runs a random failure on one or two random
// nodes for 1 minute with 1 minute recovery, for 20 cycles total. Nodes n1-n2
// are used as SQL gateways, and are not failed to avoid disconnecting the
// client workload.
//
// It runs with either a read-write or read-only KV workload, measuring the pMax
// unavailability for graphing. The read-only workload is useful to test e.g.
// recovering nodes stealing Raft leadership away, since this requires the
// replica to still be up-to-date on the log.
func runFailoverChaos(ctx context.Context, t test.Test, c cluster.Cluster, readOnly bool) {
	require.Equal(t, 10, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster, and set up failers for all failure modes.
	opts := option.DefaultStartOpts()
	opts.RoachprodOpts.ScheduleBackups = false
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_ENABLE_UNSAFE_TEST_BUILTINS=true")
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	m := c.NewMonitor(ctx, c.Range(1, 9))

	failures := []failers.Failer{}
	for _, failureMode := range failers.AllFailureModes {
		failer := failers.MakeFailerWithoutLocalNoop(t, c, t.L(), m, failureMode, opts, settings, rng)
		if c.IsLocal() && !failer.CanUseLocal() {
			t.L().Printf("skipping failure mode %q on local cluster", failureMode)
			continue
		}
		_ = failer.Setup(ctx)
		defer failer.Cleanup(ctx)
		failures = append(failures, failer)
	}

	c.Start(ctx, t.L(), opts, settings, c.Range(1, 9))

	conn := c.Conn(ctx, t.L(), 1)

	// Place 5 replicas of all ranges on n3-n9, keeping n1-n2 as SQL gateways.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 5, onlyNodes: []int{3, 4, 5, 6, 7, 8, 9}})

	// Wait for upreplication.
	require.NoError(
		t, WaitForReplication(ctx, t, t.L(), conn, 5 /* replicationFactor */, atLeastReplicationFactor),
	)

	// Create the kv database. If this is a read-only workload, populate it with
	// 100.000 keys.
	var insertCount int
	if readOnly {
		insertCount = 100000
	}
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	c.Run(ctx, option.WithNodes(c.Node(10)), fmt.Sprintf(
		`./cockroach workload init kv --splits 1000 --insert-count %d {pgurl:1}`, insertCount))

	// Scatter the ranges, then relocate them off of the SQL gateways n1-n2.
	t.L().Printf("scattering table")
	_, err = conn.ExecContext(ctx, `ALTER TABLE kv.kv SCATTER`)
	require.NoError(t, err)
	relocateRanges(t, ctx, conn, `true`, []int{1, 2}, []int{3, 4, 5, 6, 7, 8, 9})

	// Wait for upreplication of the new ranges.
	require.NoError(
		t, WaitForReplication(ctx, t, t.L(), conn, 5 /* replicationFactor */, atLeastReplicationFactor),
	)

	// Run workload on n10 via n1-n2 gateways until test ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		readPercent := 50
		if readOnly {
			readPercent = 100
		}
		err := c.RunE(ctx, option.WithNodes(c.Node(10)), fmt.Sprintf(
			`./cockroach workload run kv --read-percent %d --write-seq R%d `+
				`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
				`--histograms=`+t.PerfArtifactsDir()+`/stats.json {pgurl:1-2}`,
			readPercent, insertCount))
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to randomly fail random nodes for 1 minute, with 20 cycles.
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		for i := 0; i < 20; i++ {
			sleepFor(ctx, t, time.Minute)

			// Ranges may occasionally escape their constraints. Move them to where
			// they should be.
			relocateRanges(t, ctx, conn, `true`, []int{1, 2}, []int{3, 4, 5, 6, 7, 8, 9})

			// Randomly sleep up to the lease renewal interval, to vary the time
			// between the last lease renewal and the failure.
			sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

			// Pick 1 or 2 random nodes and failure modes. Make sure we call Ready()
			// on both before failing, since we may need to fetch information from the
			// cluster which won't work if there's an active failure.
			nodeFailers := map[int]failers.Failer{}
			for numNodes := 1 + rng.Intn(2); len(nodeFailers) < numNodes; {
				var node int
				for node == 0 || nodeFailers[node] != nil {
					node = 3 + rng.Intn(7) // n1-n2 are SQL gateways, n10 is workload runner
				}
				var failer failers.Failer
				for failer == nil {
					failer = failures[rng.Intn(len(failures))]
					for _, other := range nodeFailers {
						if !other.CanRunWith(failer.Mode()) || !failer.CanRunWith(other.Mode()) {
							failer = nil // failers aren't compatible, pick a different one
							break
						}
					}
				}
				if d, ok := failer.(*failers.DeadlockFailer); ok { // randomize deadlockFailer
					d.NumReplicas = 1 + rng.Intn(5)
					d.OnlyLeaseholders = rng.Float64() < 0.5
				}
				failer.Ready(ctx, node)
				nodeFailers[node] = failer
			}

			for node, failer := range nodeFailers {
				// If the failer supports partial failures (e.g. partial partitions), do
				// one with 50% probability against a random node (including SQL
				// gateways).
				if partialFailer, ok := failer.(failers.PartialFailer); ok && rng.Float64() < 0.5 {
					var partialPeer int
					for partialPeer == 0 || partialPeer == node {
						partialPeer = 1 + rng.Intn(9)
					}
					t.L().Printf("failing n%d to n%d (%s)", node, partialPeer, failer)
					partialFailer.FailPartial(ctx, node, []int{partialPeer})
				} else {
					t.L().Printf("failing n%d (%s)", node, failer)
					_ = failer.Fail(ctx, node)
				}
			}

			sleepFor(ctx, t, time.Minute)

			for node, failer := range nodeFailers {
				t.L().Printf("recovering n%d (%s)", node, failer)
				failer.Recover(ctx, node)
			}
		}

		sleepFor(ctx, t, time.Minute) // let cluster recover
		return nil
	})
	m.Wait()
}

// runFailoverPartialLeaseGateway tests a partial network partition between a
// SQL gateway and a user range leaseholder. These must be routed via other
// nodes to be able to serve the request.
//
// Cluster topology:
//
// n1-n3: system ranges and user ranges (2/5 replicas)
// n4-n5: user range leaseholders (2/5 replicas)
// n6-n7: SQL gateways and 1 user replica (1/5 replicas)
//
// 5 user range replicas will be placed on n2-n6, with leases on n4. A partial
// partition will be introduced between n4,n5 and n6,n7, both fully and
// individually. This corresponds to the case where we have three data centers
// with a broken network link between one pair. For example:
//
//	                        n1-n3 (2 replicas, liveness)
//	                          A
//	                        /   \
//	                       /     \
//	               n4-n5  B --x-- C  n6-n7  <---  n8 (workload)
//	(2 replicas, leases)             (1 replica, SQL gateways)
//
// Once follower routing is implemented, this tests the following scenarios:
//
// - Routes via followers in both A, B, and C when possible.
// - Skips follower replica on local node that can't reach leaseholder (n6).
// - Skips follower replica in C that can't reach leaseholder (n7 via n6).
// - Skips follower replica in B that's unreachable (n5).
//
// We run a kv50 workload on SQL gateways and collect pMax latency for graphing.
func runFailoverPartialLeaseGateway(ctx context.Context, t test.Test, c cluster.Cluster) {
	require.Equal(t, 8, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	opts := option.DefaultStartOpts()
	opts.RoachprodOpts.ScheduleBackups = false
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	m := c.NewMonitor(ctx, c.Range(1, 7))

	failer := failers.MakeFailer(t, c, t.L(), m, failers.FailureModeBlackhole, opts, settings, rng).(failers.PartialFailer)
	_ = failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Start(ctx, t.L(), opts, settings, c.Range(1, 7))

	conn := c.Conn(ctx, t.L(), 1)

	// Place all ranges on n1-n3 to start with.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})

	// Wait for upreplication.
	require.NoError(t, WaitFor3XReplication(ctx, t, t.L(), conn))

	// Create the kv database with 5 replicas on n2-n6, and leases on n4.
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	configureZone(t, ctx, conn, `DATABASE kv`, zoneConfig{
		replicas: 5, onlyNodes: []int{2, 3, 4, 5, 6}, leasePreference: "[+node4]"})

	c.Run(ctx, option.WithNodes(c.Node(6)), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// Wait for the KV table to upreplicate.
	waitForUpreplication(t, ctx, conn, `database_name = 'kv'`, 5)

	// The replicate queue takes forever to move the ranges, so we do it
	// ourselves. Precreating the database/range and moving it to the correct
	// nodes first is not sufficient, since workload will spread the ranges across
	// all nodes regardless.
	relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 7}, []int{2, 3, 4, 5, 6})
	relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{4, 5, 6, 7}, []int{1, 2, 3})
	relocateLeases(t, ctx, conn, `database_name = 'kv'`, 4)

	// Run workload on n8 via n6-n7 gateways until test ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		err := c.RunE(ctx, option.WithNodes(c.Node(8)), `./cockroach workload run kv --read-percent 50 `+
			`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			`--histograms=`+t.PerfArtifactsDir()+`/stats.json {pgurl:6-7}`)
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to fail and recover partial partitions between n4,n5
	// (leases) and n6,n7 (gateways), both fully and individually, for 3 cycles.
	// Leases are only placed on n4.
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		for i := 0; i < 3; i++ {
			testcases := []struct {
				nodes []int
				peers []int
			}{
				// Fully partition leases from gateways, must route via n1-n3. In
				// addition to n4 leaseholder being unreachable, follower on n5 is
				// unreachable, and follower replica on n6 can't reach leaseholder.
				{[]int{6, 7}, []int{4, 5}},
				// Partition n6 (gateway with local follower) from n4 (leaseholder).
				// Local follower replica can't reach leaseholder.
				{[]int{6}, []int{4}},
				// Partition n7 (gateway) from n4 (leaseholder).
				{[]int{7}, []int{4}},
			}
			for _, tc := range testcases {
				sleepFor(ctx, t, time.Minute)

				// Ranges and leases may occasionally escape their constraints. Move
				// them to where they should be.
				relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 7}, []int{2, 3, 4, 5, 6})
				relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{4, 5, 6, 7}, []int{1, 2, 3})
				relocateLeases(t, ctx, conn, `database_name = 'kv'`, 4)

				// Randomly sleep up to the lease renewal interval, to vary the time
				// between the last lease renewal and the failure.
				sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

				for _, node := range tc.nodes {
					failer.Ready(ctx, node)
				}

				for _, node := range tc.nodes {
					t.L().Printf("failing n%d to n%v (%s lease/gateway)", node, tc.peers, failer)
					failer.FailPartial(ctx, node, tc.peers)
				}

				sleepFor(ctx, t, time.Minute)

				for _, node := range tc.nodes {
					t.L().Printf("recovering n%d to n%v (%s lease/gateway)", node, tc.peers, failer)
					failer.Recover(ctx, node)
				}
			}
		}

		sleepFor(ctx, t, time.Minute) // let cluster recover
		return nil
	})
	m.Wait()
}

// runFailoverPartialLeaseLeader tests a partial network partition between
// leaseholders and Raft leaders. This will prevent the leaseholder from making
// Raft proposals, but it can still hold onto leases as long as it can heartbeat
// liveness.
//
// Cluster topology:
//
// n1-n3: system and liveness ranges, SQL gateway
// n4-n6: user ranges
//
// The cluster runs with COCKROACH_DISABLE_LEADER_FOLLOWS_LEASEHOLDER, which
// will place Raft leaders and leases independently of each other. We can then
// assume that some number of user ranges will randomly have split leader/lease,
// and simply create partial partitions between each of n4-n6 in sequence.
//
// We run a kv50 workload on SQL gateways and collect pMax latency for graphing.
func runFailoverPartialLeaseLeader(ctx context.Context, t test.Test, c cluster.Cluster) {
	require.Equal(t, 7, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster, disabling leader/leaseholder colocation. We only start
	// n1-n3, to precisely place system ranges, since we'll have to disable the
	// replicate queue shortly.
	opts := option.DefaultStartOpts()
	opts.RoachprodOpts.ScheduleBackups = false
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_DISABLE_LEADER_FOLLOWS_LEASEHOLDER=true")
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	m := c.NewMonitor(ctx, c.Range(1, 6))

	failer := failers.MakeFailer(t, c, t.L(), m, failers.FailureModeBlackhole, opts, settings, rng).(failers.PartialFailer)
	_ = failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Start(ctx, t.L(), opts, settings, c.Range(1, 3))

	conn := c.Conn(ctx, t.L(), 1)

	// Place all ranges on n1-n3 to start with, and wait for upreplication.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})

	// NB: We want to ensure the system ranges are all down-replicated from their
	// initial RF of 5, so pass in exactlyReplicationFactor below.
	require.NoError(t, WaitForReplication(ctx, t, t.L(), conn, 3, exactlyReplicationFactor))

	// Now that system ranges are properly placed on n1-n3, start n4-n6.
	c.Start(ctx, t.L(), opts, settings, c.Range(4, 6))

	// Create the kv database on n4-n6.
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	configureZone(t, ctx, conn, `DATABASE kv`, zoneConfig{replicas: 3, onlyNodes: []int{4, 5, 6}})

	c.Run(ctx, option.WithNodes(c.Node(6)), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// Move ranges to the appropriate nodes. Precreating the database/range and
	// moving it to the correct nodes first is not sufficient, since workload will
	// spread the ranges across all nodes regardless.
	relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3}, []int{4, 5, 6})
	relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{4, 5, 6}, []int{1, 2, 3})

	// Check that we have a few split leaders/leaseholders on n4-n6. We give
	// it a few seconds, since metrics are updated every 10 seconds.
	for i := 0; ; i++ {
		var count float64
		for _, node := range []int{4, 5, 6} {
			count += nodeMetric(ctx, t, c, node, "replicas.leaders_not_leaseholders")
		}
		t.L().Printf("%.0f split leaders/leaseholders", count)
		if count >= 3 {
			break
		} else if i >= 10 {
			t.Fatalf("timed out waiting for 3 split leaders/leaseholders")
		}
		time.Sleep(time.Second)
	}

	// Run workload on n7 via n1-n3 gateways until test ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		err := c.RunE(ctx, option.WithNodes(c.Node(7)), `./cockroach workload run kv --read-percent 50 `+
			`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			`--histograms=`+t.PerfArtifactsDir()+`/stats.json {pgurl:1-3}`)
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to fail and recover partial partitions between each pair of
	// n4-n6 for 3 cycles (9 failures total).
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		for i := 0; i < 3; i++ {
			for _, node := range []int{4, 5, 6} {
				sleepFor(ctx, t, time.Minute)

				// Ranges may occasionally escape their constraints. Move them to where
				// they should be.
				relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3}, []int{4, 5, 6})
				relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{4, 5, 6}, []int{1, 2, 3})

				// Randomly sleep up to the lease renewal interval, to vary the time
				// between the last lease renewal and the failure.
				sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

				failer.Ready(ctx, node)

				peer := node + 1
				if peer > 6 {
					peer = 4
				}
				t.L().Printf("failing n%d to n%d (%s lease/leader)", node, peer, failer)
				failer.FailPartial(ctx, node, []int{peer})

				sleepFor(ctx, t, time.Minute)

				t.L().Printf("recovering n%d to n%d (%s lease/leader)", node, peer, failer)
				failer.Recover(ctx, node)
			}
		}

		sleepFor(ctx, t, time.Minute) // let cluster recover
		return nil
	})
	m.Wait()
}

// runFailoverPartialLeaseLiveness tests a partial network partition between a
// leaseholder and node liveness. With epoch leases we would normally expect
// this to recover shortly, since the node can't heartbeat its liveness record
// and thus its leases will expire. However, it will maintain Raft leadership,
// and we prevent non-leaders from acquiring leases, which can prevent the lease
// from moving unless we explicitly handle this. See also:
// https://github.com/cockroachdb/cockroach/pull/87244.
//
// Cluster topology:
//
// n1-n3: system ranges and SQL gateways
// n4:    liveness leaseholder
// n5-7:  user ranges
//
// A partial blackhole network partition is triggered between n4 and each of
// n5-n7 sequentially, 3 times per node for a total of 9 times. A kv50 workload
// is running against SQL gateways on n1-n3, and we collect the pMax latency for
// graphing.
func runFailoverPartialLeaseLiveness(ctx context.Context, t test.Test, c cluster.Cluster) {
	require.Equal(t, 8, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	opts := option.DefaultStartOpts()
	opts.RoachprodOpts.ScheduleBackups = false
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	m := c.NewMonitor(ctx, c.Range(1, 7))

	failer := failers.MakeFailer(t, c, t.L(), m, failers.FailureModeBlackhole, opts, settings, rng).(failers.PartialFailer)
	_ = failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Start(ctx, t.L(), opts, settings, c.Range(1, 7))

	conn := c.Conn(ctx, t.L(), 1)

	// Place all ranges on n1-n3, and an extra liveness leaseholder replica on n4.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})
	configureZone(t, ctx, conn, `RANGE liveness`, zoneConfig{
		replicas: 4, onlyNodes: []int{1, 2, 3, 4}, leasePreference: "[+node4]"})

	// Wait for upreplication.
	require.NoError(t, WaitFor3XReplication(ctx, t, t.L(), conn))

	// Create the kv database on n5-n7.
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	configureZone(t, ctx, conn, `DATABASE kv`, zoneConfig{replicas: 3, onlyNodes: []int{5, 6, 7}})

	c.Run(ctx, option.WithNodes(c.Node(6)), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// The replicate queue takes forever to move the ranges, so we do it
	// ourselves. Precreating the database/range and moving it to the correct
	// nodes first is not sufficient, since workload will spread the ranges across
	// all nodes regardless.
	relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3, 4}, []int{5, 6, 7})
	relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{5, 6, 7}, []int{1, 2, 3, 4})
	relocateRanges(t, ctx, conn, `range_id != 2`, []int{4}, []int{1, 2, 3})

	// Run workload on n8 using n1-n3 as gateways (not partitioned) until test
	// ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		err := c.RunE(ctx, option.WithNodes(c.Node(8)), `./cockroach workload run kv --read-percent 50 `+
			`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			`--histograms=`+t.PerfArtifactsDir()+`/stats.json {pgurl:1-3}`)
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to fail and recover partial partitions between n4 (liveness)
	// and workload leaseholders n5-n7 for 1 minute each, 3 times per node for 9
	// times total.
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		for i := 0; i < 3; i++ {
			for _, node := range []int{5, 6, 7} {
				sleepFor(ctx, t, time.Minute)

				// Ranges and leases may occasionally escape their constraints. Move
				// them to where they should be.
				relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3, 4}, []int{5, 6, 7})
				relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{node}, []int{1, 2, 3})
				relocateRanges(t, ctx, conn, `range_id = 2`, []int{5, 6, 7}, []int{1, 2, 3, 4})
				relocateLeases(t, ctx, conn, `range_id = 2`, 4)

				// Randomly sleep up to the lease renewal interval, to vary the time
				// between the last lease renewal and the failure.
				sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

				failer.Ready(ctx, node)

				peer := 4
				t.L().Printf("failing n%d to n%d (%s lease/liveness)", node, peer, failer)
				failer.FailPartial(ctx, node, []int{peer})

				sleepFor(ctx, t, time.Minute)

				t.L().Printf("recovering n%d to n%d (%s lease/liveness)", node, peer, failer)
				failer.Recover(ctx, node)
			}
		}

		sleepFor(ctx, t, time.Minute) // let cluster recover
		return nil
	})
	m.Wait()
}

// runFailoverNonSystem benchmarks the maximum duration of range unavailability
// following a leaseholder failure with only non-system ranges.
//
//   - No system ranges located on the failed node.
//
//   - SQL clients do not connect to the failed node.
//
//   - The workload consists of individual point reads and writes.
//
// The cluster layout is as follows:
//
// n1-n3: System ranges and SQL gateways.
// n4-n6: Workload ranges.
// n7:    Workload runner.
//
// The test runs a kv50 workload via gateways on n1-n3, measuring the pMax
// latency for graphing.
func runFailoverNonSystem(
	ctx context.Context, t test.Test, c cluster.Cluster, failureMode failers.FailureMode,
) {
	require.Equal(t, 7, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	opts := option.DefaultStartOpts()
	opts.RoachprodOpts.ScheduleBackups = false
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_ENABLE_UNSAFE_TEST_BUILTINS=true")
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	m := c.NewMonitor(ctx, c.Range(1, 6))

	failer := failers.MakeFailer(t, c, t.L(), m, failureMode, opts, settings, rng)
	_ = failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Start(ctx, t.L(), opts, settings, c.Range(1, 6))

	conn := c.Conn(ctx, t.L(), 1)

	// Constrain all existing zone configs to n1-n3.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})

	// Wait for upreplication.
	require.NoError(t, WaitFor3XReplication(ctx, t, t.L(), conn))

	// Create the kv database, constrained to n4-n6. Despite the zone config, the
	// ranges will initially be distributed across all cluster nodes.
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	configureZone(t, ctx, conn, `DATABASE kv`, zoneConfig{replicas: 3, onlyNodes: []int{4, 5, 6}})
	c.Run(ctx, option.WithNodes(c.Node(7)), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// The replicate queue takes forever to move the kv ranges from n1-n3 to
	// n4-n6, so we do it ourselves. Precreating the database/range and moving it
	// to the correct nodes first is not sufficient, since workload will spread
	// the ranges across all nodes regardless.
	relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3}, []int{4, 5, 6})

	// Run workload on n7 via n1-n3 gateways until test ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		err := c.RunE(ctx, option.WithNodes(c.Node(7)), `./cockroach workload run kv --read-percent 50 `+
			`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			`--histograms=`+t.PerfArtifactsDir()+`/stats.json {pgurl:1-3}`)
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to fail and recover n4-n6 in order.
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		for i := 0; i < 3; i++ {
			for _, node := range []int{4, 5, 6} {
				sleepFor(ctx, t, time.Minute)

				// Ranges may occasionally escape their constraints. Move them
				// to where they should be.
				relocateRanges(t, ctx, conn, `database_name = 'kv'`, []int{1, 2, 3}, []int{4, 5, 6})
				relocateRanges(t, ctx, conn, `database_name != 'kv'`, []int{node}, []int{1, 2, 3})

				// Randomly sleep up to the lease renewal interval, to vary the time
				// between the last lease renewal and the failure.
				sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

				failer.Ready(ctx, node)

				t.L().Printf("failing n%d (%s)", node, failer)
				_ = failer.Fail(ctx, node)

				sleepFor(ctx, t, time.Minute)

				t.L().Printf("recovering n%d (%s)", node, failer)
				failer.Recover(ctx, node)
			}
		}
		return nil
	})
	m.Wait()
}

// runFailoverLiveness benchmarks the maximum duration of *user* range
// unavailability following a liveness-only leaseholder failure. When the
// liveness range becomes unavailable, other nodes are unable to heartbeat and
// extend their leases, and their leases may thus expire as well making them
// unavailable.
//
//   - Only liveness range located on the failed node, as leaseholder.
//
//   - SQL clients do not connect to the failed node.
//
//   - The workload consists of individual point reads and writes.
//
// The cluster layout is as follows:
//
// n1-n3: All ranges, including liveness.
// n4:    Liveness range leaseholder.
// n5:    Workload runner.
//
// The test runs a kv50 workload via gateways on n1-n3, measuring the pMax
// latency for graphing.
func runFailoverLiveness(
	ctx context.Context, t test.Test, c cluster.Cluster, failureMode failers.FailureMode,
) {
	require.Equal(t, 5, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	opts := option.DefaultStartOpts()
	opts.RoachprodOpts.ScheduleBackups = false
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_ENABLE_UNSAFE_TEST_BUILTINS=true")
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	m := c.NewMonitor(ctx, c.Range(1, 4))

	failer := failers.MakeFailer(t, c, t.L(), m, failureMode, opts, settings, rng)
	_ = failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Start(ctx, t.L(), opts, settings, c.Range(1, 4))

	conn := c.Conn(ctx, t.L(), 1)

	// Constrain all existing zone configs to n1-n3.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})

	// Constrain the liveness range to n1-n4, with leaseholder preference on n4.
	configureZone(t, ctx, conn, `RANGE liveness`, zoneConfig{replicas: 4, leasePreference: "[+node4]"})

	// Wait for upreplication.
	require.NoError(t, WaitFor3XReplication(ctx, t, t.L(), conn))

	// Create the kv database, constrained to n1-n3. Despite the zone config, the
	// ranges will initially be distributed across all cluster nodes.
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	configureZone(t, ctx, conn, `DATABASE kv`, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})
	c.Run(ctx, option.WithNodes(c.Node(5)), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// The replicate queue takes forever to move the other ranges off of n4 so we
	// do it ourselves. Precreating the database/range and moving it to the
	// correct nodes first is not sufficient, since workload will spread the
	// ranges across all nodes regardless.
	relocateRanges(t, ctx, conn, `range_id != 2`, []int{4}, []int{1, 2, 3})

	// We also make sure the lease is located on n4.
	relocateLeases(t, ctx, conn, `range_id = 2`, 4)

	// Run workload on n5 via n1-n3 gateways until test ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		err := c.RunE(ctx, option.WithNodes(c.Node(5)), `./cockroach workload run kv --read-percent 50 `+
			`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			`--histograms=`+t.PerfArtifactsDir()+`/stats.json {pgurl:1-3}`)
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to fail and recover n4.
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		for i := 0; i < 9; i++ {
			sleepFor(ctx, t, time.Minute)

			// Ranges and leases may occasionally escape their constraints. Move them
			// to where they should be.
			relocateRanges(t, ctx, conn, `range_id != 2`, []int{4}, []int{1, 2, 3})
			relocateLeases(t, ctx, conn, `range_id = 2`, 4)

			// Randomly sleep up to the lease renewal interval, to vary the time
			// between the last lease renewal and the failure.
			sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

			failer.Ready(ctx, 4)

			t.L().Printf("failing n%d (%s)", 4, failer)
			_ = failer.Fail(ctx, 4)

			sleepFor(ctx, t, time.Minute)

			t.L().Printf("recovering n%d (%s)", 4, failer)
			failer.Recover(ctx, 4)
			relocateLeases(t, ctx, conn, `range_id = 2`, 4)
		}

		sleepFor(ctx, t, time.Minute) // let cluster recover
		return nil
	})
	m.Wait()
}

// runFailoverSystemNonLiveness benchmarks the maximum duration of range
// unavailability following a leaseholder failure with only system ranges,
// excluding the liveness range which is tested separately in
// runFailoverLiveness.
//
//   - No user or liveness ranges located on the failed node.
//
//   - SQL clients do not connect to the failed node.
//
//   - The workload consists of individual point reads and writes.
//
// The cluster layout is as follows:
//
// n1-n3: Workload ranges, liveness range, and SQL gateways.
// n4-n6: System ranges excluding liveness.
// n7:    Workload runner.
//
// The test runs a kv50 workload via gateways on n1-n3, measuring the pMax
// latency for graphing.
func runFailoverSystemNonLiveness(
	ctx context.Context, t test.Test, c cluster.Cluster, failureMode failers.FailureMode,
) {
	require.Equal(t, 7, c.Spec().NodeCount)

	rng, _ := randutil.NewTestRand()

	// Create cluster.
	opts := option.DefaultStartOpts()
	opts.RoachprodOpts.ScheduleBackups = false
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_ENABLE_UNSAFE_TEST_BUILTINS=true")
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=100ms") // speed up replication

	m := c.NewMonitor(ctx, c.Range(1, 6))

	failer := failers.MakeFailer(t, c, t.L(), m, failureMode, opts, settings, rng)
	_ = failer.Setup(ctx)
	defer failer.Cleanup(ctx)

	c.Start(ctx, t.L(), opts, settings, c.Range(1, 6))

	conn := c.Conn(ctx, t.L(), 1)

	// Constrain all existing zone configs to n4-n6, except liveness which is
	// constrained to n1-n3.
	configureAllZones(t, ctx, conn, zoneConfig{replicas: 3, onlyNodes: []int{4, 5, 6}})
	configureZone(t, ctx, conn, `RANGE liveness`, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})

	// Wait for upreplication.
	require.NoError(t, WaitFor3XReplication(ctx, t, t.L(), conn))

	// Create the kv database, constrained to n1-n3. Despite the zone config, the
	// ranges will initially be distributed across all cluster nodes.
	t.L().Printf("creating workload database")
	_, err := conn.ExecContext(ctx, `CREATE DATABASE kv`)
	require.NoError(t, err)
	configureZone(t, ctx, conn, `DATABASE kv`, zoneConfig{replicas: 3, onlyNodes: []int{1, 2, 3}})
	c.Run(ctx, option.WithNodes(c.Node(7)), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	// The replicate queue takes forever to move the kv ranges from n4-n6 to
	// n1-n3, so we do it ourselves. Precreating the database/range and moving it
	// to the correct nodes first is not sufficient, since workload will spread
	// the ranges across all nodes regardless.
	relocateRanges(t, ctx, conn, `database_name = 'kv' OR range_id = 2`,
		[]int{4, 5, 6}, []int{1, 2, 3})
	relocateRanges(t, ctx, conn, `database_name != 'kv' AND range_id != 2`,
		[]int{1, 2, 3}, []int{4, 5, 6})

	// Run workload on n7 via n1-n3 as gateways until test ends (context cancels).
	t.L().Printf("running workload")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		err := c.RunE(ctx, option.WithNodes(c.Node(7)), `./cockroach workload run kv --read-percent 50 `+
			`--concurrency 256 --max-rate 2048 --timeout 1m --tolerate-errors `+
			`--histograms=`+t.PerfArtifactsDir()+`/stats.json {pgurl:1-3}`)
		if ctx.Err() != nil {
			return nil // test requested workload shutdown
		}
		return err
	})

	// Start a worker to fail and recover n4-n6 in order.
	m.Go(func(ctx context.Context) error {
		defer cancelWorkload()

		for i := 0; i < 3; i++ {
			for _, node := range []int{4, 5, 6} {
				sleepFor(ctx, t, time.Minute)

				// Ranges may occasionally escape their constraints. Move them
				// to where they should be.
				relocateRanges(t, ctx, conn, `database_name != 'kv' AND range_id != 2`,
					[]int{1, 2, 3}, []int{4, 5, 6})
				relocateRanges(t, ctx, conn, `database_name = 'kv' OR range_id = 2`,
					[]int{4, 5, 6}, []int{1, 2, 3})

				// Randomly sleep up to the lease renewal interval, to vary the time
				// between the last lease renewal and the failure.
				sleepFor(ctx, t, randutil.RandDuration(rng, rangeLeaseRenewalDuration))

				failer.Ready(ctx, node)

				t.L().Printf("failing n%d (%s)", node, failer)
				_ = failer.Fail(ctx, node)

				sleepFor(ctx, t, time.Minute)

				t.L().Printf("recovering n%d (%s)", node, failer)
				failer.Recover(ctx, node)
			}
		}

		sleepFor(ctx, t, time.Minute) // let cluster recover
		return nil
	})
	m.Wait()
}

// waitForUpreplication waits for upreplication of ranges that satisfy the
// given predicate (using SHOW RANGES).
//
// TODO(erikgrinaker): move this into WaitForReplication() when it can use SHOW
// RANGES, i.e. when it's no longer needed in mixed-version tests with older
// versions that don't have SHOW RANGES.
func waitForUpreplication(
	t test.Test, ctx context.Context, conn *gosql.DB, predicate string, replicationFactor int,
) {
	var count int
	where := fmt.Sprintf("WHERE array_length(replicas, 1) < %d", replicationFactor)
	if predicate != "" {
		where += fmt.Sprintf(" AND (%s)", predicate)
	}
	for {
		require.NoError(t, conn.QueryRowContext(ctx,
			`SELECT count(distinct range_id) FROM [SHOW CLUSTER RANGES WITH TABLES, DETAILS] `+where).
			Scan(&count))
		if count == 0 {
			break
		}
		t.L().Printf("waiting for %d ranges to upreplicate (%s)", count, predicate)
		time.Sleep(time.Second)
	}
}

// relocateRanges relocates all ranges matching the given predicate from a set
// of nodes to a different set of nodes. Moves are attempted sequentially from
// each source onto each target, and errors are retried indefinitely.
//
// TODO(erikgrinaker): It would be really neat if the replicate queue could
// deal with this for us. For that to happen, we need three things:
//
//  1. The replicate queue must do this ~immediately. It should take ~10 seconds
//     for 1000 ranges, not 10 minutes.
//
//  2. We need to know when the replicate queue is done placing both replicas and
//     leases in accordance with the zone configurations, so that we can wait for
//     it. SpanConfigConformance should provide this, but currently doesn't have
//     a public API, and doesn't handle lease preferences.
//
//  3. The replicate queue must guarantee that replicas and leases won't escape
//     their constraints after the initial setup. We see them do so currently.
func relocateRanges(
	t test.Test, ctx context.Context, conn *gosql.DB, predicate string, from, to []int,
) {
	require.NotEmpty(t, predicate)
	var count int
	for _, source := range from {
		where := fmt.Sprintf("(%s) AND %d = ANY(replicas)", predicate, source)
		for {
			require.NoError(t, conn.QueryRowContext(ctx,
				`SELECT count(distinct range_id) FROM [SHOW CLUSTER RANGES WITH TABLES] WHERE `+where).
				Scan(&count))
			if count == 0 {
				break
			}
			t.L().Printf("moving %d ranges off of n%d (%s)", count, source, predicate)
			for _, target := range to {
				_, err := conn.ExecContext(ctx, `ALTER RANGE RELOCATE FROM $1::int TO $2::int FOR `+
					`SELECT DISTINCT range_id FROM [SHOW CLUSTER RANGES WITH TABLES] WHERE `+where,
					source, target)
				if err != nil {
					t.L().Printf("failed to move ranges: %s", err)
				}
			}
			time.Sleep(time.Second)
		}
	}
}

// relocateLeases relocates all leases matching the given predicate to the
// given node. Errors and failures are retried indefinitely.
func relocateLeases(t test.Test, ctx context.Context, conn *gosql.DB, predicate string, to int) {
	require.NotEmpty(t, predicate)
	var count int
	where := fmt.Sprintf("%s AND lease_holder != %d", predicate, to)
	for {
		require.NoError(t, conn.QueryRowContext(ctx,
			`SELECT count(distinct range_id) FROM [SHOW CLUSTER RANGES WITH TABLES, DETAILS] WHERE `+
				where).
			Scan(&count))
		if count == 0 {
			break
		}
		t.L().Printf("moving %d leases to n%d (%s)", count, to, predicate)
		_, err := conn.ExecContext(ctx, `ALTER RANGE RELOCATE LEASE TO $1::int FOR `+
			`SELECT DISTINCT range_id FROM [SHOW CLUSTER RANGES WITH TABLES, DETAILS] WHERE `+where, to)
		if err != nil {
			t.L().Printf("failed to move leases: %s", err)
		}
		time.Sleep(time.Second)
	}
}

type zoneConfig struct {
	replicas        int
	onlyNodes       []int
	leasePreference string
}

// configureZone sets the zone config for the given target.
func configureZone(
	t test.Test, ctx context.Context, conn *gosql.DB, target string, cfg zoneConfig,
) {
	require.NotZero(t, cfg.replicas, "num_replicas must be > 0")

	// If onlyNodes is given, invert the constraint and specify which nodes are
	// prohibited. Otherwise, the allocator may leave replicas outside of the
	// specified nodes.
	var constraintsString string
	if len(cfg.onlyNodes) > 0 {
		nodeCount := t.Spec().(*registry.TestSpec).Cluster.NodeCount - 1 // subtract worker node
		included := map[int]bool{}
		for _, nodeID := range cfg.onlyNodes {
			included[nodeID] = true
		}
		excluded := []int{}
		for nodeID := 1; nodeID <= nodeCount; nodeID++ {
			if !included[nodeID] {
				excluded = append(excluded, nodeID)
			}
		}
		for _, nodeID := range excluded {
			if len(constraintsString) > 0 {
				constraintsString += ","
			}
			constraintsString += fmt.Sprintf("-node%d", nodeID)
		}
	}

	_, err := conn.ExecContext(ctx, fmt.Sprintf(
		`ALTER %s CONFIGURE ZONE USING num_replicas = %d, constraints = '[%s]', lease_preferences = '[%s]'`,
		target, cfg.replicas, constraintsString, cfg.leasePreference))
	require.NoError(t, err)
}

// configureAllZones will set zone configuration for all targets in the
// clusters.
func configureAllZones(t test.Test, ctx context.Context, conn *gosql.DB, cfg zoneConfig) {
	rows, err := conn.QueryContext(ctx, `SELECT target FROM [SHOW ALL ZONE CONFIGURATIONS]`)
	require.NoError(t, err)
	for rows.Next() {
		var target string
		require.NoError(t, rows.Scan(&target))
		configureZone(t, ctx, conn, target, cfg)
	}
	require.NoError(t, rows.Err())
}

// nodeMetric fetches the given metric value from the given node.
func nodeMetric(
	ctx context.Context, t test.Test, c cluster.Cluster, node int, metric string,
) float64 {
	var value float64
	conn := c.Conn(ctx, t.L(), node)
	defer conn.Close()
	err := conn.QueryRowContext(
		ctx, `SELECT value FROM crdb_internal.node_metrics WHERE name = $1`, metric).Scan(&value)
	require.NoError(t, err)
	return value
}

// sleepFor sleeps for the given duration. The test fails on context cancellation.
func sleepFor(ctx context.Context, t test.Test, duration time.Duration) {
	select {
	case <-time.After(duration):
	case <-ctx.Done():
		t.Fatalf("sleep failed: %s", ctx.Err())
	}
}
