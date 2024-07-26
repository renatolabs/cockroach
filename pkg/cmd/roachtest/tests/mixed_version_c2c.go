// Copyright 2023 The Cockroach Authors.
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
	"math/rand"
	"net/url"
	"sync"
	"time"

	apd "github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func registerC2CMixedVersions(r registry.Registry) {

	sp := replicationSpec{
		srcNodes: 4,
		dstNodes: 4,
		// The timeout field ensures the c2c roachtest driver behaves properly.
		timeout:                   10 * time.Minute,
		workload:                  replicateKV{readPercent: 0, debugRunDuration: 1 * time.Minute, maxBlockBytes: 1, initWithSplitAndScatter: true},
		additionalDuration:        0 * time.Minute,
		cutover:                   30 * time.Second,
		skipNodeDistributionCheck: true,
		clouds:                    registry.OnlyGCE,
		suites:                    registry.Suites(registry.Nightly),
	}

	r.Add(registry.TestSpec{
		Name:             "c2c/mixed-version",
		Owner:            registry.OwnerDisasterRecovery,
		Cluster:          r.MakeClusterSpec(sp.dstNodes+sp.srcNodes+1, spec.WorkloadNode()),
		CompatibleClouds: sp.clouds,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runC2CMixedVersions(ctx, t, c, sp)
		},
	})
}

const (
	expectedMajorUpgrades = 1
	minSupportedVersion   = "v23.2.0"
	replicationJobType    = "REPLICATION STREAM INGESTION"
	fingerprintQuery      = `SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM VIRTUAL CLUSTER $1 WITH START TIMESTAMP = '%s'] AS OF SYSTEM TIME '%s'`
)

// TODO (msbutler): schedule upgrades during initial scan and cutover.
func runC2CMixedVersions(ctx context.Context, t test.Test, c cluster.Cluster, sp replicationSpec) {
	cm := InitC2CMixed(ctx, t, c, sp)
	cm.SetupHook(ctx)
	cm.WorkloadHook(ctx)
	cm.UpdateHook(ctx)
	cm.Run(ctx, c)
}

func InitC2CMixed(
	ctx context.Context, t test.Test, c cluster.Cluster, sp replicationSpec,
) *c2cMixed {
	sourceMvt := mixedversion.NewTest(ctx, t, t.L(), c, c.Range(1, sp.srcNodes),
		mixedversion.MinimumSupportedVersion(minSupportedVersion),
		mixedversion.AlwaysUseLatestPredecessors,
		mixedversion.NumUpgrades(expectedMajorUpgrades),
		mixedversion.EnabledDeploymentModes(mixedversion.SharedProcessDeployment),
		mixedversion.WithTag("source"),
		mixedversion.DisableSkipVersionUpgrades,
	)

	destMvt := mixedversion.NewTest(ctx, t, t.L(), c, c.Range(sp.srcNodes+1, sp.srcNodes+sp.dstNodes),
		mixedversion.MinimumSupportedVersion(minSupportedVersion),
		mixedversion.AlwaysUseLatestPredecessors,
		mixedversion.NumUpgrades(expectedMajorUpgrades),
		mixedversion.EnabledDeploymentModes(mixedversion.SharedProcessDeployment),
		mixedversion.WithTag("dest"),
		mixedversion.DisableSkipVersionUpgrades,
	)
	return &c2cMixed{
		sourceMvt:           sourceMvt,
		destMvt:             destMvt,
		sourceStartedChan:   make(chan struct{}),
		destStartedChan:     make(chan struct{}),
		sp:                  sp,
		t:                   t,
		c:                   c,
		fingerprintArgsChan: make(chan fingerprintArgs, 1),
		fingerprintChan:     make(chan int64, 1),
	}
}

type sourceTenantInfo struct {
	name  string
	pgurl *url.URL
}

type fingerprintArgs struct {
	retainedTime hlc.Timestamp
	cutoverTime  hlc.Timestamp
}

type c2cMixed struct {
	sourceMvt, destMvt  *mixedversion.Test
	sourceStartedChan   chan struct{}
	destStartedChan     chan struct{}
	sp                  replicationSpec
	t                   test.Test
	c                   cluster.Cluster
	fingerprintArgsChan chan fingerprintArgs
	fingerprintChan     chan int64

	startTime       hlc.Timestamp
	ingestionJobID  catpb.JobID
	workloadStopper mixedversion.StopFunc
}

func (cm *c2cMixed) SetupHook(ctx context.Context) {
	sourceChan := make(chan sourceTenantInfo, 1)

	cm.sourceMvt.OnStartup(
		"generate pgurl",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			// Enable rangefeeds, required for PCR to work.
			l.Printf("enabling rangefeeds")
			if err := h.System.Exec(r, "SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
				return errors.Wrap(err, "failed to enable rangefeeds")
			}
			close(cm.sourceStartedChan)

			l.Printf("generating pgurl")
			srcNode := cm.c.Node(1)
			srcClusterSetting := install.MakeClusterSettings()
			addr, err := cm.c.ExternalPGUrl(ctx, l, srcNode, roachprod.PGURLOptions{
				VirtualClusterName: install.SystemInterfaceName,
			})
			if err != nil {
				return err
			}

			pgURL, err := copyPGCertsAndMakeURL(ctx, cm.t, cm.c, srcNode, srcClusterSetting.PGUrlCertsDir, addr[0])
			if err != nil {
				return err
			}

			l.Printf("sending pgurl via channel: %s", pgURL)
			sourceChan <- sourceTenantInfo{name: h.Tenant.Descriptor.Name, pgurl: pgURL}

			l.Printf("waiting for destination tenant to be created")
			<-cm.destStartedChan
			l.Printf("done")

			return nil
		},
	)

	cm.destMvt.CreateTenantFunc(func(
		ctx context.Context,
		l *logger.Logger,
		r *rand.Rand,
		h *mixedversion.Helper,
		name string,
	) error {
		l.Printf("waiting to hear from source cluster")
		sourceInfo := <-sourceChan

		if err := h.System.Exec(r, fmt.Sprintf(
			"CREATE TENANT %q FROM REPLICATION OF %q ON '%s'",
			name, sourceInfo.name, sourceInfo.pgurl.String(),
		)); err != nil {
			return errors.Wrap(err, "creating destionation tenant")
		}

		if err := h.System.QueryRow(r, fmt.Sprintf(
			"SELECT job_id FROM [SHOW JOBS] WHERE job_type = '%s'",
			replicationJobType,
		)).Scan(&cm.ingestionJobID); err != nil {
			return errors.Wrap(err, "querying ingestion job ID")
		}

		l.Printf("replication job: %d", cm.ingestionJobID)
		close(cm.destStartedChan)
		return nil
	})
}

func (cm *c2cMixed) WorkloadHook(ctx context.Context) {
	tpccInitCmd := roachtestutil.NewCommand("./cockroach workload init tpcc").
		Arg("{pgurl%s}", cm.c.Range(1, cm.sp.srcNodes))
	tpccRunCmd := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", cm.c.Range(1, cm.sp.srcNodes)).
		Option("tolerate-errors").
		Flag("warehouses", 100)
	cm.workloadStopper = cm.sourceMvt.Workload("tpcc", cm.c.WorkloadNode(), tpccInitCmd, tpccRunCmd)
}

func (cm *c2cMixed) UpdateHook(ctx context.Context) {
	destFinalized := make(chan struct{}, 1)

	// For a given major version update this can be called three times: upgrade, downgrade, upgrade again
	cm.sourceMvt.InMixedVersion(
		"wait for dest to finalize",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			if h.Context().Stage == mixedversion.LastUpgradeStage {
				l.Printf("waiting for destination cluster to finalize upgrade")
				<-destFinalized
			}

			return nil
		},
	)

	// Called at the end of each major version upgrade.
	destMajorUpgradeCount := 0
	cm.destMvt.AfterUpgradeFinalized(
		"cutover and allow source to finalize",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			// Ensure the source always waits to finalize until after the dest finalizes.
			destFinalized <- struct{}{}
			destMajorUpgradeCount++
			if destMajorUpgradeCount == expectedMajorUpgrades {
				return cm.destCutoverAndFingerprint(ctx, l, r, h)
			}
			return nil
		},
	)

	sourceMajorUpgradeCount := 0
	cm.sourceMvt.AfterUpgradeFinalized(
		"fingerprint source",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			sourceMajorUpgradeCount++
			if sourceMajorUpgradeCount == expectedMajorUpgrades {
				return cm.sourceFingerprint(ctx, l, r, h)
			}
			return nil
		},
	)
}

func (cm *c2cMixed) destCutoverAndFingerprint(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {

	var retainedTime hlc.Timestamp
	if err := h.System.QueryRow(r,
		`SELECT retained_time FROM [SHOW TENANT $1 WITH REPLICATION STATUS]`, h.Tenant.Descriptor.Name).Scan(&retainedTime); err != nil {
		return err
	}

	var cutoverStr string
	if err := h.System.QueryRow(r, "ALTER TENANT $1 COMPLETE REPLICATION TO LATEST", h.Tenant.Descriptor.Name).Scan(&cutoverStr); err != nil {
		return err
	}
	cutover, err := stringToHLC(cutoverStr)
	if err != nil {
		return err
	}
	_, db := h.System.RandomDB(r)
	if err := WaitForSucceed(ctx, db, cm.ingestionJobID, time.Minute); err != nil {
		return err
	}

	l.Printf("Retained time %s; cutover time %s", retainedTime.GoTime(), cutover.GoTime())
	cm.fingerprintArgsChan <- fingerprintArgs{
		retainedTime: retainedTime,
		cutoverTime:  cutover,
	}
	var destFingerprint int64
	if err := h.System.QueryRow(r, fmt.Sprintf(fingerprintQuery, retainedTime.AsOfSystemTime(), cutover.AsOfSystemTime())).Scan(&destFingerprint); err != nil {
		return err
	}
	cm.fingerprintChan <- destFingerprint
	return nil
}

func (cm *c2cMixed) sourceFingerprint(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {
	args := <-cm.fingerprintArgsChan
	cm.workloadStopper()
	var sourceFingerprint int64
	if err := h.System.QueryRow(r, fmt.Sprintf(fingerprintQuery, args.retainedTime.AsOfSystemTime(), args.cutoverTime.AsOfSystemTime())).Scan(&sourceFingerprint); err != nil {
		return err
	}

	destFingerprint := <-cm.fingerprintChan
	if sourceFingerprint != destFingerprint {
		return errors.Newf("source fingerprint %d does not match dest fingerprint %d", sourceFingerprint, destFingerprint)
	}
	return nil
}

func (cm *c2cMixed) Run(ctx context.Context, c cluster.Cluster) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				cm.t.L().Printf("source cluster upgrade failed: %v", r)
			}
		}()
		defer wg.Done()
		cm.sourceMvt.Run()
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				cm.t.L().Printf("destination cluster upgrade failed: %v", r)
			}
		}()
		defer wg.Done()

		<-cm.sourceStartedChan
		cm.destMvt.Run()
	}()

	wg.Wait()
}

func stringToHLC(s string) (hlc.Timestamp, error) {
	d, _, err := apd.NewFromString(s)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	ts, err := hlc.DecimalToHLC(d)
	return ts, err
}
