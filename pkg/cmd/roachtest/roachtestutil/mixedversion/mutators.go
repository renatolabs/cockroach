// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mixedversion

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/failers"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

const (
	// PreserveDowngradeOptionRandomizer is a mutator that changes the
	// timing in which the `preserve_downgrade_option` cluster setting
	// is reset during the upgrade test. Typically (when this mutator is
	// not enabled), this happens at the end of the `LastUpgrade`
	// stage, when all nodes have been restarted and are running the
	// target binary version. However, we have observed bugs in the past
	// that only manifested when this setting was reset at different
	// times in the test (e.g., #111610); this mutator exists to catch
	// regressions of this type.
	PreserveDowngradeOptionRandomizer = "preserve_downgrade_option_randomizer"
)

type preserveDowngradeOptionRandomizerMutator struct{}

func (m preserveDowngradeOptionRandomizerMutator) Name() string {
	return PreserveDowngradeOptionRandomizer
}

// Most runs will have this mutator disabled, as the base upgrade
// plan's approach of resetting the cluster setting when all nodes are
// upgraded is the most sensible / common.
func (m preserveDowngradeOptionRandomizerMutator) Probability() float64 {
	return 0.3
}

// Generate returns mutations to remove the existing step to reset the
// `preserve_downgrade_option` cluster setting, and reinserts it back
// in some other point in the test, before all nodes are upgraded. Not
// every upgrade in the test plan is affected, but the upgrade to the
// current version is always mutated.
func (m preserveDowngradeOptionRandomizerMutator) Generate(
	rng *rand.Rand, plan *TestPlan,
) []mutation {
	var mutations []mutation
	for _, upgradeSelector := range randomUpgrades(rng, plan) {
		removeExistingStep := upgradeSelector.
			Filter(func(s *singleStep) bool {
				_, ok := s.impl.(allowUpgradeStep)
				return ok
			}).
			Remove()

		addRandomly := upgradeSelector.
			Filter(func(s *singleStep) bool {
				// It is valid to reset the cluster setting when we are
				// performing a rollback (as we know the next upgrade will be
				// the final one); or during the final upgrade itself.
				return (s.context.Stage == LastUpgradeStage || s.context.Stage == RollbackUpgradeStage) &&
					// We also don't want all nodes to be running the latest
					// binary, as that would be equivalent to the test plan
					// without this mutator.
					len(s.context.NodesInNextVersion()) < len(s.context.CockroachNodes)
			}).
			RandomStep(rng).
			// Note that we don't attempt a concurrent insert because the
			// selected step could be one that restarts a cockroach node,
			// and `allowUpgradeStep` could fail in that situation.
			InsertBefore(allowUpgradeStep{})

		// Finally, we update the context associated with every step where
		// all nodes are running the next verison to indicate they are in
		// fact in `Finalizing` state. Previously, this would only be set
		// after `allowUpgradeStep` but, when this mutator is enabled,
		// `Finalizing` should be `true` as soon as all nodes are on the
		// next version.
		for _, step := range upgradeSelector.
			Filter(func(s *singleStep) bool {
				return len(s.context.NodesInNextVersion()) == len(s.context.CockroachNodes)
			}) {
			step.context.Finalizing = true
		}

		mutations = append(mutations, removeExistingStep...)
		mutations = append(mutations, addRandomly...)
	}

	return mutations
}

// randomUpgrades returns selectors for the steps of a random subset
// of upgrades in the plan. The last upgrade is always returned, as
// that is the most critical upgrade being tested.
func randomUpgrades(rng *rand.Rand, plan *TestPlan) []stepSelector {
	allUpgrades := plan.allUpgrades()
	numChanges := rng.Intn(len(allUpgrades)) // other than last upgrade
	allExceptLastUpgrade := append([]*upgradePlan{}, allUpgrades[:len(allUpgrades)-1]...)

	rng.Shuffle(len(allExceptLastUpgrade), func(i, j int) {
		allExceptLastUpgrade[i], allExceptLastUpgrade[j] = allExceptLastUpgrade[j], allExceptLastUpgrade[i]
	})

	byUpgrade := func(upgrade *upgradePlan) func(*singleStep) bool {
		return func(s *singleStep) bool {
			return s.context.FromVersion.Equal(upgrade.from)
		}
	}

	// By default, include the last upgrade.
	selectors := []stepSelector{
		plan.newStepSelector().Filter(byUpgrade(allUpgrades[len(allUpgrades)-1])),
	}
	for _, upgrade := range allExceptLastUpgrade[:numChanges] {
		selectors = append(selectors, plan.newStepSelector().Filter(byUpgrade(upgrade)))
	}

	return selectors
}

const FaultInjectorMutator = "fault_injector"

type faultInjectorMutator struct{}

func (m faultInjectorMutator) Name() string {
	return FaultInjectorMutator
}

// Most runs will have this mutator disabled, as the base upgrade
// plan's approach of resetting the cluster setting when all nodes are
// upgraded is the most sensible / common.
func (m faultInjectorMutator) Probability() float64 {
	return 1 // TODO: testing
}

// Generate returns mutations to remove the existing step to reset the
// `preserve_downgrade_option` cluster setting, and reinserts it back
// in some other point in the test, before all nodes are upgraded. Not
// every upgrade in the test plan is affected, but the upgrade to the
// current version is always mutated.
func (m faultInjectorMutator) Generate(rng *rand.Rand, plan *TestPlan) []mutation {
	var mutations []mutation
	setupFailers := make(map[failers.FailureMode]struct{})
	for _, upgradeSelector := range randomUpgrades(rng, plan) {
		duringMigrationsProbability := 0.9
		failDuringMigrations := rng.Float64() < duringMigrationsProbability
		failureCount := 1
		if failDuringMigrations {
			failureCount = 5
		}
		mode, setupStep, runStep := newFaultInjectionStep(rng, failureCount)

		var insertSetupStep []mutation
		if _, seen := setupFailers[mode]; !seen {
			insertSetupStep = plan.newStepSelector().
				Filter(func(s *singleStep) bool {
					return s.context.Stage == OnStartupStage
				}).
				RandomStep(rng).
				Insert(rng, setupStep)

			setupFailers[mode] = struct{}{}
		}

		mutations = append(mutations, insertSetupStep...)

		var insertRunStep []mutation
		if failDuringMigrations {
			insertRunStep = upgradeSelector.
				Filter(func(s *singleStep) bool {
					return s.context.Stage == RunningUpgradeMigrationsStage
				}).
				RandomStep(rng).
				InsertBefore(runStep)

			mutations = append(mutations, insertRunStep...)
		} else {
			insertRunStep = upgradeSelector.
				Filter(func(s *singleStep) bool {
					return s.context.Stage != RunningUpgradeMigrationsStage
				}).
				RandomStep(rng).
				InsertSequential(rng, runStep)

			mutations = append(mutations, insertRunStep...)
		}
	}

	return mutations
}

func newFaultInjectionStep(
	rng *rand.Rand, count int,
) (failers.FailureMode, singleStepProtocol, singleStepProtocol) {
	allFailures := []failers.FailureMode{
		failers.FailureModeBlackholeRecv,
		failers.FailureModeBlackholeSend,
		failers.FailureModeDiskStall,
		failers.FailureModePause,
	}
	mode := allFailures[rng.Intn(len(allFailures))]

	possibleDurations := []time.Duration{
		10 * time.Second, 1 * time.Minute, 5 * time.Minute,
	}
	if mode == failers.FailureModeDiskStall {
		possibleDurations = []time.Duration{5 * time.Second, 10 * time.Second}
	}

	duration := possibleDurations[rng.Intn(len(possibleDurations))]

	setupStep := setupFaultInjectorStep{mode: mode}
	runStep := runFaultInjectorStep{mode: mode, duration: duration, count: count}

	return mode, setupStep, runStep
}

type setupFaultInjectorStep struct {
	mode failers.FailureMode
}

func (s setupFaultInjectorStep) Description() string {
	return fmt.Sprintf("set up fault injector %q", s.mode)
}

func (s setupFaultInjectorStep) Background() shouldStop { return nil }

func (s setupFaultInjectorStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	l.Printf("set up fault injector %s", s.mode)
	failer := failers.MakeFailer(
		// No test.Test: intentional; should be returning error.
		nil, h.runner.cluster, l, nil, s.mode, option.DefaultStartOpts(), install.ClusterSettings{}, rng,
	)
	return failer.Setup(ctx)
}

type runFaultInjectorStep struct {
	mode     failers.FailureMode
	duration time.Duration
	count    int
}

func (s runFaultInjectorStep) Description() string {
	var timesStr string
	if s.count > 1 {
		timesStr = fmt.Sprintf(" a total of %d times", s.count)
	}
	return fmt.Sprintf(
		"apply fault injector %q on some node for %s%s",
		s.mode, s.duration, timesStr,
	)
}

func (s runFaultInjectorStep) Background() shouldStop { return nil }

func (s runFaultInjectorStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	delayBetweenFails := 2 * time.Minute
	failer := failers.MakeFailer(
		// No test.Test: intentional; should be returning error.
		nil, h.runner.cluster, l, nil, s.mode, option.DefaultStartOpts(), install.ClusterSettings{}, rng,
	)

	runFailer := func() error {
		target := h.RandomNode(rng, h.Context.CockroachNodes)
		l.Printf("running fault injection %s on n%d for %s", s.mode, target, s.duration)

		if err := failer.Fail(ctx, target); err != nil {
			return err
		}

		defer failer.Recover(ctx, target)

		select {
		case <-time.After(s.duration):
		case <-ctx.Done():
		}

		return nil
	}

	for j := 0; j < s.count; j++ {
		if err := runFailer(); err != nil {
			return err
		}

		l.Printf("waiting for %s", delayBetweenFails)
		select {
		case <-time.After(delayBetweenFails):
		case <-ctx.Done():
		}
	}

	return nil
}
