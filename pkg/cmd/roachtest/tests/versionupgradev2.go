package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type (
	hookFunc      func(context.Context, *logger.Logger) error
	predicateFunc func(option.NodeListOption, option.NodeListOption, bool) bool

	versionUpgradeHook struct {
		name      string
		predicate predicateFunc
		fn        hookFunc
	}

	MixedVersionTest struct {
		ctx           context.Context
		cluster       cluster.Cluster
		t             test.Test
		logger        *logger.Logger
		legacyUpgrade *versionUpgradeTest

		crdbNodes       option.NodeListOption
		binaryVersions  []roachpb.Version
		clusterVersions []roachpb.Version

		previousRelease    string
		predecessorVersion roachpb.Version

		currentlyInstalled string
		versionsChanged    int

		prng *rand.Rand
		rngs []*RandomNumberGen

		hooks        []versionUpgradeHook
		allowUpgrade bool
	}

	RandomNumberGen struct {
		numNodes int
		n        int
	}
)

const (
	currentVersion               = ""
	logPrefix                    = "mixed-version-test"
	runWhileMigratingProbability = 0.5
)

// TODO: ideas
//  - randomly go multiple releases back
//  - pick patch release randomly
func NewMixedVersionTest(
	ctx context.Context, t test.Test, c cluster.Cluster, crdbNodes option.NodeListOption,
) *MixedVersionTest {
	buildVersion := *t.BuildVersion()
	previousRelease, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}
	predParts := strings.Split(previousRelease, ".")
	predecessorVersion, err := roachpb.ParseVersion(fmt.Sprintf("%s.%s", predParts[0], predParts[1]))
	if err != nil {
		t.Fatal(err)
	}

	prng, seed := randutil.NewPseudoRand()
	t.L().Printf("mixed-version random seed: %d", seed)

	return &MixedVersionTest{
		ctx:                ctx,
		cluster:            c,
		t:                  t,
		logger:             prefixedLogger(t, logPrefix),
		crdbNodes:          crdbNodes,
		previousRelease:    previousRelease,
		predecessorVersion: predecessorVersion,
		legacyUpgrade:      newVersionUpgradeTest(c),
		prng:               prng,
	}
}

func (mvt *MixedVersionTest) newRandomNumberGenerator() *RandomNumberGen {
	rng := &RandomNumberGen{numNodes: len(mvt.crdbNodes)}
	mvt.rngs = append(mvt.rngs, rng)

	return rng
}

func (mvt *MixedVersionTest) RegisterHook(desc string, pred predicateFunc, fn hookFunc) {
	mvt.hooks = append(mvt.hooks, versionUpgradeHook{desc, pred, fn})
}

func (mvt *MixedVersionTest) InMixedVersion(desc string, fn hookFunc) {
	mvt.hooks = append(mvt.hooks, mvt.newMixedVersionHook(desc, fn))
}

func (mvt *MixedVersionTest) OnStartup(desc string, fn hookFunc) {
	predicate := func(newVersionNodes, _ option.NodeListOption, allowUpgrade bool) bool {
		return len(newVersionNodes) == 0 && !allowUpgrade
	}

	mvt.hooks = append(mvt.hooks, versionUpgradeHook{desc, predicate, fn})
}

func (mvt *MixedVersionTest) SQLConn() *gosql.DB {
	node := 1 // TODO randomize
	return mvt.legacyUpgrade.conn(mvt.ctx, mvt.t, node)
}

// pred -> main -> pred -> main + finalize
func (mvt *MixedVersionTest) Run() {
	mvt.startFromCheckpoint()
	mvt.step(lowerLeaseDuration(1))     // TODO randomize node
	mvt.step(preventAutoUpgradeStep(1)) // TODO randomize node
	mvt.changeVersion("upgrading", currentVersion)
	mvt.changeVersion("downgrading", mvt.previousRelease)
	mvt.changeVersion("upgrading (again)", currentVersion)
	mvt.finalizeUpgrade()
}

func (mvt *MixedVersionTest) changeVersion(action string, version string) {
	mvt.versionsChanged++
	targetVersion := versionMsg(version)
	mvt.logger.Printf("%s %d nodes to %s", action, len(mvt.crdbNodes), targetVersion)
	mvt.resetRngs()
	mvt.runHooks(targetVersion)

	for _, node := range mvt.crdbNodes { // TODO: randomize
		mvt.logger.Printf("%s node %d", action, node)
		nodes := option.NodeListOption{node}
		upgrades.upgradeNodes(
			mvt.ctx, nodes, option.DefaultStartOpts(), version, mvt.t, mvt.cluster, mvt.logger,
		)

		mvt.refreshBinaryVersions()
		mvt.runHooks(targetVersion)
	}

	mvt.currentlyInstalled = targetVersion
}

func (mvt *MixedVersionTest) finalizeUpgrade() {
	mvt.allowUpgrade = true
	mvt.step(allowAutoUpgradeStep(1)) // TODO randomize node
	mvt.runHooks(versionMsg(currentVersion))
	mvt.step(waitForUpgradeStep(mvt.crdbNodes))
}

func (mvt *MixedVersionTest) runHooks(targetVersion string) {
	var oldVersion, newVersion option.NodeListOption
	for node, version := range mvt.binaryVersions {
		if version.LessEq(mvt.predecessorVersion) {
			oldVersion = append(oldVersion, node+1)
		} else {
			newVersion = append(newVersion, node+1)
		}
	}

	monitor := mvt.cluster.NewMonitor(mvt.ctx, mvt.crdbNodes)
	var runnable []versionUpgradeHook
	for _, hook := range mvt.hooks {
		if hook.predicate(newVersion, oldVersion, mvt.allowUpgrade) {
			runnable = append(runnable, hook)
		}
	}

	mvt.refreshClusterVersions()
	for _, hook := range runnable {
		hook := hook // capture range variable
		monitor.Go(func(ctx context.Context) error {
			delay := mvt.prng.Intn(500)
			time.Sleep(time.Duration(delay) * time.Millisecond)

			mvt.logger.Printf("starting: %q (%s)", hook.name, mvt.formatBinaryVersions())
			start := timeutil.Now()
			defer func() {
				mvt.logger.Printf("finished: %q (%s)", hook.name, timeutil.Since(start))
			}()
			if err := hook.fn(ctx, mvt.loggerForHook(hook.name, targetVersion)); err != nil {
				return fmt.Errorf("%q failed: %w", hook.name, err)
			}

			return nil
		})
	}

	if err := monitor.WaitE(); err != nil {
		mvt.t.Fatal(mvt.wrapErr(err))
	}
}

func (mvt *MixedVersionTest) startFromCheckpoint() {
	mvt.logger.Printf("starting from checkpoint for version %s", mvt.previousRelease)
	mvt.step(uploadAndStartFromCheckpointFixture(mvt.crdbNodes, mvt.previousRelease))
	mvt.step(waitForUpgradeStep(mvt.crdbNodes))
	mvt.refreshBinaryVersions()

	mvt.currentlyInstalled = mvt.previousRelease
}

func (mvt *MixedVersionTest) refreshBinaryVersions() {
	mvt.binaryVersions = make([]roachpb.Version, 0, len(mvt.crdbNodes))
	for _, node := range mvt.crdbNodes {
		version := mvt.legacyUpgrade.binaryVersion(mvt.ctx, mvt.t, node)
		mvt.binaryVersions = append(mvt.binaryVersions, version)
	}
}

func (mvt *MixedVersionTest) refreshClusterVersions() {
	mvt.clusterVersions = make([]roachpb.Version, 0, len(mvt.crdbNodes))
	for _, node := range mvt.crdbNodes {
		version := mvt.legacyUpgrade.clusterVersion(mvt.ctx, mvt.t, node)
		mvt.clusterVersions = append(mvt.clusterVersions, version)
	}
}

func (mvt *MixedVersionTest) wrapErr(err error) error {
	errMsg := fmt.Sprintf("mixed-version test failed: %s", err)
	clusterVersionsBefore := fmt.Sprintf("cluster versions before failure: %s", mvt.formatClusterVersions())
	mvt.refreshClusterVersions() // TODO: don't fail the test if this step fails
	clusterVersionsAfter := fmt.Sprintf("cluster versions after failure: %s", mvt.formatClusterVersions())

	return fmt.Errorf("%s\n%s\n%s\n%s",
		errMsg, mvt.formatBinaryVersions(), clusterVersionsBefore, clusterVersionsAfter,
	)
}

func (mvt *MixedVersionTest) formatBinaryVersions() string {
	return formatVersions("binaryVersions", mvt.binaryVersions)
}

func (mvt *MixedVersionTest) formatClusterVersions() string {
	return formatVersions("clusterVersions", mvt.clusterVersions)
}

func formatVersions(prefix string, versions []roachpb.Version) string {
	var pairs []string
	for idx, version := range versions {
		pairs = append(pairs, fmt.Sprintf("%d: %s", idx+1, version))
	}

	return fmt.Sprintf("%s: [%s]", prefix, strings.Join(pairs, ", "))
}

func (mvt *MixedVersionTest) newMixedVersionHook(desc string, fn hookFunc) versionUpgradeHook {
	rng := mvt.newRandomNumberGenerator()
	predicate := func(newVersionNodes, _ option.NodeListOption, allowUpgrade bool) bool {
		if allowUpgrade {
			return mvt.prng.Float64() < runWhileMigratingProbability
		}

		return len(newVersionNodes) == rng.N()
	}

	return versionUpgradeHook{desc, predicate, fn}
}

func (mvt *MixedVersionTest) loggerForHook(name, targetVersion string) *logger.Logger {
	upgradeDir := fmt.Sprintf("%d_%s_to_%s", mvt.versionsChanged, mvt.currentlyInstalled, targetVersion)
	prefix := fmt.Sprintf("%s/%s/%s", logPrefix, upgradeDir, name)
	return prefixedLogger(mvt.t, prefix)
}

func (mvt *MixedVersionTest) resetRngs() {
	for _, rng := range mvt.rngs {
		rng.reset(mvt.prng)
	}
}

// TODO: remove this function
func (mvt *MixedVersionTest) step(s versionStep) {
	s(mvt.ctx, mvt.t, mvt.legacyUpgrade)
}

func (rng *RandomNumberGen) N() int {
	return rng.n
}

func (rng *RandomNumberGen) reset(generator *rand.Rand) {
	rng.n = generator.Intn(rng.numNodes) + 1
}

func prefixedLogger(t test.Test, prefix string) *logger.Logger {
	fileName := strings.ReplaceAll(prefix, " ", "-")
	formattedPrefix := fmt.Sprintf("[%s]", fileName)
	logger, err := t.L().ChildLogger(fileName, logger.LogPrefix(formattedPrefix))
	if err != nil {
		t.Fatal(fmt.Errorf("failed to create logger for %q: %w", prefix, err))
	}

	return logger
}

func versionMsg(version string) string {
	if version == "" {
		return "current"
	}

	return version
}
