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
	gosql "database/sql"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const (
	noRowsFingerprint = "EMPTY"
	nullFingerprint   = "NULL"
)

var (
	invalidVersionRE = regexp.MustCompile(`[^a-zA-Z0-9\.]`)
	invalidDBNameRE  = regexp.MustCompile(`[\-\.:/]`)

	// retry options while waiting for a backup to complete
	backupCompletionRetryOptions = retry.Options{
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     1 * time.Minute,
		Multiplier:     1.5,
		MaxRetries:     50,
	}

	systemTablesInFullClusterBackup = []string{
		"users", "zones", "settings", "locations", "role_members", "role_options",
		"ui", "comments", "scheduled_jobs", "database_role_settings", "tenant_settings",
		"privileges", "external_connections",
	}
)

// sanitizeVersionForBackup takes the string representation of a
// version and removes any characters that would not be allowed in a
// backup destination.
func sanitizeVersionForBackup(v string) string {
	return invalidVersionRE.ReplaceAllString(clusterupgrade.VersionMsg(v), "")
}

type (
	// backupOption is an option passed to the `BACKUP` command (i.e.,
	// `WITH ...` portion).
	backupOption interface {
		fmt.Stringer
	}

	// revisionHistory wraps the `revision_history` backup option.
	revisionHistory struct{}

	// encryptionPassphrase is the `encryption_passphrase` backup
	// option. If passed when a backup is created, the same passphrase
	// needs to be passed for incremental backups and for restoring the
	// backup as well.
	encryptionPassphrase struct {
		passphrase string
	}

	backupType interface {
		Desc() string
		BackupCommand() string
		RestoreCommand(string) (string, []string)
		TargetTables() []string
	}

	tableBackup struct {
		db     string
		target string
	}

	databaseBackup struct {
		db     string
		tables []string
	}

	clusterBackup struct {
		dbBackup     *databaseBackup
		systemTables []string
	}

	tableFingerprints      map[string]string
	collectionFingerprints struct {
		mu   syncutil.Mutex
		data map[string]tableFingerprints
		outs map[string]string
	}

	// backupCollection wraps a backup collection (which may or may not
	// contain incremental backups). The associated fingerprint is the
	// expected fingerprint when the corresponding table is restored.
	backupCollection struct {
		btype            backupType
		name             string
		dataFingerprints *collectionFingerprints
		options          []backupOption
	}

	fullBackup struct {
		label string
	}
	incrementalBackup struct {
		collection backupCollection
	}

	// labeledNodes allows us to label a set of nodes with the version
	// they are running, to allow for human-readable backup names
	labeledNodes struct {
		Nodes   option.NodeListOption
		Version string
	}

	// backupSpec indicates where backups are supposed to be planned
	// (`BACKUP` statement sent to); and where they are supposed to be
	// executed (where the backup job will be picked up).
	backupSpec struct {
		Plan    labeledNodes
		Execute labeledNodes
	}
)

func tableNamesWithDB(db string, tables []string) []string {
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		parts := strings.Split(t, ".")
		var tableName string
		if len(parts) == 1 {
			tableName = parts[0]
		} else {
			tableName = parts[1]
		}

		names = append(names, fmt.Sprintf("%s.%s", db, tableName))
	}

	return names
}

func pickRandomTables(rng *rand.Rand, tables []string, num int) []string {
	rng.Shuffle(len(tables), func(i, j int) {
		tables[i], tables[j] = tables[j], tables[i]
	})

	return tables[:num]
}

func (fb fullBackup) String() string        { return "full" }
func (ib incrementalBackup) String() string { return "incremental" }

func (rh revisionHistory) String() string {
	return "revision_history"
}

func newEncryptionPassphrase(rng *rand.Rand) encryptionPassphrase {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	const passLen = 32

	pass := make([]byte, passLen)
	for i := range pass {
		pass[i] = alphabet[rng.Intn(len(alphabet))]
	}

	return encryptionPassphrase{string(pass)}
}

func (ep encryptionPassphrase) String() string {
	return fmt.Sprintf("encryption_passphrase = '%s'", ep.passphrase)
}

// newBackupOptions returns a list of backup options to be used when
// creating a new backup. Each backup option has a 50% chance of being
// included.
func newBackupOptions(rng *rand.Rand) []backupOption {
	possibleOpts := []backupOption{
		revisionHistory{},
		newEncryptionPassphrase(rng),
	}

	var options []backupOption
	for _, opt := range possibleOpts {
		if rng.Float64() < 0.5 {
			options = append(options, opt)
		}
	}

	return options
}

func newTableBackup(rng *rand.Rand, db string, tables []string) *tableBackup {
	target := tables[rng.Intn(len(tables))]
	return &tableBackup{db, target}
}

func (tb *tableBackup) Desc() string {
	return fmt.Sprintf("table %s.%s", tb.db, tb.target)
}

func (tb *tableBackup) BackupCommand() string {
	return fmt.Sprintf("BACKUP TABLE %s", tb.TargetTables()[0])
}

func (tb *tableBackup) RestoreCommand(uniqueName string) (string, []string) {
	options := []string{
		fmt.Sprintf("into_db = '%s'", uniqueName),
	}

	return fmt.Sprintf("RESTORE TABLE %s", tb.TargetTables()[0]), options
}

func (tb *tableBackup) TargetTables() []string {
	return []string{fmt.Sprintf("%s.%s", tb.db, tb.target)}
}

func newDatabaseBackup(db string, tables []string) *databaseBackup {
	return &databaseBackup{db, tables}
}

func (dbb *databaseBackup) Desc() string {
	return fmt.Sprintf("database %s", dbb.db)
}

func (dbb *databaseBackup) BackupCommand() string {
	return fmt.Sprintf("BACKUP DATABASE %s", dbb.db)
}

func (dbb *databaseBackup) RestoreCommand(uniqueName string) (string, []string) {
	options := []string{
		fmt.Sprintf("new_db_name = '%s'", uniqueName),
	}

	return fmt.Sprintf("RESTORE DATABASE %s", dbb.db), options
}

func (dbb *databaseBackup) TargetTables() []string {
	return tableNamesWithDB(dbb.db, dbb.tables)
}

func newClusterBackup(db string, tables []string) *clusterBackup {
	return &clusterBackup{
		dbBackup:     newDatabaseBackup(db, tables),
		systemTables: systemTablesInFullClusterBackup,
	}
}

func (cb *clusterBackup) Desc() string                               { return "cluster" }
func (cb *clusterBackup) BackupCommand() string                      { return "BACKUP" }
func (cb *clusterBackup) RestoreCommand(_ string) (string, []string) { return "RESTORE", nil }

func (cb *clusterBackup) TargetTables() []string {
	return append(cb.dbBackup.TargetTables(), tableNamesWithDB("system", cb.systemTables)...)
}

func (bc *backupCollection) uri() string {
	return fmt.Sprintf("gs://cockroachdb-backup-testing/%s?AUTH=implicit", bc.name)
}

func (bc *backupCollection) checkRestoredFingerprints(
	l *logger.Logger, restored *collectionFingerprints, load func(string) (string, error),
) error {
	var hadError bool
	for table, fingerprints := range bc.dataFingerprints.data {
		restoredFingerprints, ok := restored.data[table]
		if !ok {
			return fmt.Errorf("missing restored fingerprints for table %s", table)
		}

		var fingerprintsCompared int
		for index, origFingerprint := range fingerprints {
			restoredFingerprint, ok := restoredFingerprints[index]
			if !ok {
				continue
			}

			if origFingerprint != restoredFingerprint {
				out, err := load(table)
				if err != nil {
					return err
				}
				before := bc.dataFingerprints.outs[table]
				l.Printf(
					"backup %s: mismatched fingerprints for table %s, index %s.\n"+
						"--- Expected: %s\n--- Actual:   %s\n--- Contents before:\n%s\n--- Contents after:\n%s",
					bc.name, table, index, origFingerprint, restoredFingerprint, before, out,
				)
				hadError = true
			}
			fingerprintsCompared++
		}

		if fingerprintsCompared == 0 {
			return fmt.Errorf("backup %s: no shared index for table %s", bc.name, table)
		}
	}

	if hadError {
		return fmt.Errorf("fingerprint errors happened, see longs")
	}
	return nil
}

// backupCollectionDesc builds a string that describes how a backup
// collection comprised of a full backup and a follow-up incremental
// backup was generated (in terms of which versions planned vs
// executed the backup). Used to generate descriptive backup names.
func backupCollectionDesc(fullSpec, incSpec backupSpec) string {
	specMsg := func(label string, s backupSpec) string {
		if s.Plan.Version == s.Execute.Version {
			return fmt.Sprintf("%s planned and executed on %s", label, s.Plan.Version)
		}

		return fmt.Sprintf("%s planned on %s executed on %s", label, s.Plan.Version, s.Execute.Version)
	}

	if reflect.DeepEqual(fullSpec, incSpec) {
		return specMsg("all", fullSpec)
	}

	return fmt.Sprintf("%s %s", specMsg("full", fullSpec), specMsg("incremental", incSpec))
}

func newCollectionFingerprints() *collectionFingerprints {
	return &collectionFingerprints{
		data: make(map[string]tableFingerprints),
		outs: make(map[string]string),
	}
}

func (cf *collectionFingerprints) addTableFingerprints(name string, tf tableFingerprints) {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	cf.data[name] = tf
}

func (cf *collectionFingerprints) addOut(name, out string) {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	cf.outs[name] = out
}

// mixedVersionBackup is the struct that contains all the necessary
// state involved in the mixed-version backup test.
type mixedVersionBackup struct {
	cluster    cluster.Cluster
	roachNodes option.NodeListOption
	// backup collections that are created along the test
	collections []*backupCollection
	// the db being backed up/restored
	db     string
	tables []string
	// counter that is incremented atomically to provide unique
	// identifiers to backups created during the test
	currentBackupID int64

	stopWorkload mixedversion.StopFunc
}

func newMixedVersionBackup(
	c cluster.Cluster, roachNodes option.NodeListOption, db string,
) *mixedVersionBackup {
	mvb := &mixedVersionBackup{cluster: c, db: db, roachNodes: roachNodes}
	return mvb
}

func (mvb *mixedVersionBackup) newBackupType(rng *rand.Rand) backupType {
	possibleTypes := []backupType{
		// newTableBackup(rng, mvb.db, mvb.tables),
		// newDatabaseBackup(mvb.db, mvb.tables),
		newClusterBackup(mvb.db, mvb.tables),
	}

	return possibleTypes[rng.Intn(len(possibleTypes))]
}

// setShortJobIntervals increases the frequency of the adopt and
// cancel loops in the job registry. This enables changes to job state
// to be observed faster, and the test to run quicker.
func (*mixedVersionBackup) setShortJobIntervals(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	return setShortJobIntervalsCommon(func(query string, args ...interface{}) error {
		return h.Exec(rng, query, args...)
	})
}

func (mvb *mixedVersionBackup) loadTables(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) ([]string, error) {
	node, db := h.RandomDB(rng, mvb.roachNodes)
	l.Printf("loading table information for DB %q via node %d", mvb.db, node)
	query := fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s]", mvb.db)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to read tables for database %s: %w", mvb.db, err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("error scanning table_name for db %s: %w", mvb.db, err)
		}
		tables = append(tables, name)
	}

	l.Printf("database %q has %d tables", mvb.db, len(tables))
	return tables, nil
}

// loadBackupData starts a CSV server in the background and runs
// imports a bank fixture. Blocks until the importing is finished, at
// which point the CSV server is terminated.
func (mvb *mixedVersionBackup) loadBackupData(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	var rows int
	if mvb.cluster.IsLocal() {
		rows = 100
		l.Printf("importing only %d rows (as this is a local run)", rows)
	} else {
		rows = rows3GiB
		l.Printf("importing %d rows", rows)
	}

	csvPort := 8081
	importNode := h.RandomNode(rng, mvb.roachNodes)
	l.Printf("decided to run import on node %d", importNode)

	currentRoach := mixedversion.CurrentCockroachPath
	stopCSVServer := h.Background("csv server", func(bgCtx context.Context, bgLogger *logger.Logger) error {
		cmd := importBankCSVServerCommand(currentRoach, csvPort)
		bgLogger.Printf("running CSV server in the background: %q", cmd)
		if err := mvb.cluster.RunE(bgCtx, mvb.roachNodes, cmd); err != nil {
			return fmt.Errorf("error while running csv server: %w", err)
		}

		return nil
	})
	if err := waitForPort(ctx, mvb.roachNodes, csvPort, mvb.cluster); err != nil {
		return err
	}

	err := mvb.cluster.RunE(
		ctx,
		mvb.cluster.Node(importNode),
		importBankCommand(currentRoach, rows, 0 /* ranges */, csvPort, importNode),
	)

	stopCSVServer()
	if err != nil {
		return err
	}

	mvb.tables, err = mvb.loadTables(ctx, l, rng, h)
	return err
}

// randomWait waits from 1s to 5m, to allow for the background
// workload to update the underlying table we are backing up.
func (mvb *mixedVersionBackup) randomWait(l *logger.Logger, rng *rand.Rand) {
	durations := []int{1, 10, 60, 5 * 60}
	dur := time.Duration(durations[rng.Intn(len(durations))]) * time.Second
	l.Printf("waiting for %s", dur)
	time.Sleep(dur)
}

func (mvb *mixedVersionBackup) now() string {
	return hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}.AsOfSystemTime()
}

func (mvb *mixedVersionBackup) nextID() int64 {
	return atomic.AddInt64(&mvb.currentBackupID, 1)
}

// backupName returns a descriptive name for a backup depending on the
// state of the test we are in. The given label is also used to
// provide more context. Example: '3_22.2.4-to-current_final'
func (mvb *mixedVersionBackup) backupName(
	id int64, h *mixedversion.Helper, label string, btype backupType,
) string {
	testContext := h.Context()
	var finalizing string
	if testContext.Finalizing {
		finalizing = "_finalizing"
	}

	fromVersion := sanitizeVersionForBackup(testContext.FromVersion)
	toVersion := sanitizeVersionForBackup(testContext.ToVersion)
	sanitizedLabel := strings.ReplaceAll(label, " ", "-")
	sanitizedType := strings.ReplaceAll(btype.Desc(), " ", "-")

	return fmt.Sprintf(
		"%d_%s-to-%s_%s_%s%s",
		id, fromVersion, toVersion, sanitizedType, sanitizedLabel, finalizing,
	)
}

// waitForJobSuccess waits for the given job with the given ID to
// succeed (according to `backupCompletionRetryOptions`). Returns an
// error if the job doesn't succeed within the attempted retries.
func (mvb *mixedVersionBackup) waitForJobSuccess(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper, jobID int,
) error {
	var lastErr error
	node, db := h.RandomDB(rng, mvb.roachNodes)
	l.Printf("querying job status through node %d", node)
	for r := retry.StartWithCtx(ctx, backupCompletionRetryOptions); r.Next(); {
		var status string
		var payloadBytes []byte
		err := db.QueryRow(`SELECT status, payload FROM system.jobs WHERE id = $1`, jobID).Scan(&status, &payloadBytes)
		if err != nil {
			lastErr = fmt.Errorf("error reading (status, payload) for job %d: %w", jobID, err)
			l.Printf("%v", lastErr)
			continue
		}

		if jobs.Status(status) == jobs.StatusFailed {
			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
				lastErr = fmt.Errorf("job %d failed with error: %s", jobID, payload.Error)
			} else {
				lastErr = fmt.Errorf("job %d failed, and could not unmarshal payload: %w", jobID, err)
			}

			l.Printf("%v", lastErr)
			break
		}

		if expected, actual := jobs.StatusSucceeded, jobs.Status(status); expected != actual {
			lastErr = fmt.Errorf("job %d: current status %q, waiting for %q", jobID, actual, expected)
			l.Printf("%v", lastErr)
			continue
		}

		l.Printf("job %d: success", jobID)
		return nil
	}

	return fmt.Errorf("waiting for job to finish: %w", lastErr)
}

// computeFingerprints returns the fingerprint for a set of tables; if
// a non-empty `timestamp` is passed, the fingerprints are calculated
// as of that timestamp.
func (mvb *mixedVersionBackup) computeFingerprints(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	tables []string,
	timestamp string,
	h *mixedversion.Helper,
) (*collectionFingerprints, error) {
	node, db := h.RandomDB(rng, mvb.roachNodes)
	l.Printf("querying fingerprints through node %d", node)

	allFingerprints := newCollectionFingerprints()
	eg, _ := errgroup.WithContext(ctx)
	for _, table := range tables {
		table := table // capture range variable
		eg.Go(func() error {
			l.Printf("computing fingerprint for table %s", table)
			var aost string
			if timestamp != "" {
				aost = fmt.Sprintf(" AS OF SYSTEM TIME '%s'", timestamp)
			}

			query := fmt.Sprintf(
				"SELECT index_name, fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s]%s",
				table, aost,
			)
			rows, err := db.QueryContext(ctx, query)
			if err != nil {
				return fmt.Errorf("error when running query [%s]: %w", query, err)
			}
			defer rows.Close()

			currentFingerprints := make(tableFingerprints)
			for rows.Next() {
				var indexName, actualFingerprint string
				var fprint gosql.NullString
				if err := rows.Scan(&indexName, &fprint); err != nil {
					if errors.Is(err, gosql.ErrNoRows) {
						actualFingerprint = noRowsFingerprint
					} else {
						return fmt.Errorf("error computing fingerprint for table %s, index %s: %w", table, indexName, err)
					}
				}

				if actualFingerprint == "" {
					if fprint.Valid {
						actualFingerprint = fprint.String
					} else {
						actualFingerprint = nullFingerprint
					}
				}
				currentFingerprints[indexName] = actualFingerprint
			}

			outQuery := fmt.Sprintf("SELECT * FROM %s%s", table, aost)
			cmd := fmt.Sprintf("%s sql --insecure -e '%s'", mixedversion.CurrentCockroachPath, outQuery)
			if out, err := mvb.cluster.RunWithDetailsSingleNode(ctx, l, option.NodeListOption{node}, cmd); err != nil {
				return fmt.Errorf("failed to run query %q: %w", outQuery, err)
			} else {
				allFingerprints.addOut(table, out.Stdout)
			}

			allFingerprints.addTableFingerprints(table, currentFingerprints)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return allFingerprints, nil
}

// saveFingerprint stores the backup collection in the
// `mixedVersionBackup` struct, along with the associated fingerprints.
func (mvb *mixedVersionBackup) saveFingerprints(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	collection *backupCollection,
	timestamp string,
	h *mixedversion.Helper,
) error {
	fprints, err := mvb.computeFingerprints(ctx, l, rng, collection.btype.TargetTables(), timestamp, h)
	if err != nil {
		return fmt.Errorf("error computing fingerprint for backup %s: %w", collection.name, err)
	}

	collection.dataFingerprints = fprints
	l.Printf("computed fingerprints for %d tables for %s", len(fprints.data), collection.name)
	mvb.collections = append(mvb.collections, collection)
	return nil
}

// runBackup runs a `BACKUP` statement; the backup type `bType` needs
// to be an instance of either `fullBackup` or
// `incrementalBackup`. Returns when the backup job has completed.
func (mvb *mixedVersionBackup) runBackup(
	ctx context.Context,
	l *logger.Logger,
	bType fmt.Stringer,
	rng *rand.Rand,
	nodes option.NodeListOption,
	h *mixedversion.Helper,
) (backupCollection, string, error) {
	tc := h.Context()
	if !tc.Finalizing {
		// don't wait if upgrade is finalizing to increase the chances of
		// creating a backup while upgrade migrations are being run.
		mvb.randomWait(l, rng)
	}

	// NB: we need to run with the `detached` option + poll the
	// `system.jobs` table because we are intentionally disabling job
	// adoption in some nodes in the mixed-version test. Running the job
	// without the `detached` option will cause a `node liveness` error
	// to be returned when running the `BACKUP` statement.
	options := []string{"detached"}

	var latest string
	var collection backupCollection
	switch b := bType.(type) {
	case fullBackup:
		btype := mvb.newBackupType(rng)
		name := mvb.backupName(mvb.nextID(), h, b.label, btype)
		createOptions := newBackupOptions(rng)
		collection = backupCollection{
			btype:   btype,
			name:    name,
			options: createOptions,
		}
	case incrementalBackup:
		collection = b.collection
		latest = " LATEST IN"
	}

	for _, opt := range collection.options {
		options = append(options, opt.String())
	}

	backupTime := mvb.now()
	node, db := h.RandomDB(rng, nodes)

	stmt := fmt.Sprintf(
		"%s INTO%s '%s' AS OF SYSTEM TIME '%s' WITH %s",
		collection.btype.BackupCommand(),
		latest,
		collection.uri(),
		backupTime,
		strings.Join(options, ", "),
	)
	l.Printf("creating %s backup via node %d: %s", bType, node, stmt)
	var jobID int
	if err := db.QueryRowContext(ctx, stmt).Scan(&jobID); err != nil {
		return backupCollection{}, "", fmt.Errorf("error while creating %s backup %s: %w", bType, collection.name, err)
	}

	l.Printf("waiting for job %d (%s)", jobID, collection.name)
	if err := mvb.waitForJobSuccess(ctx, l, rng, h, jobID); err != nil {
		return backupCollection{}, "", err
	}

	return collection, backupTime, nil
}

// runJobOnOneOf disables job adoption on cockroach nodes that are not
// in the `nodes` list. The function passed is then executed and job
// adoption is re-enabled at the end of the function. The function
// passed is expected to run statements that trigger job creation.
func (mvb *mixedVersionBackup) runJobOnOneOf(
	ctx context.Context,
	l *logger.Logger,
	nodes option.NodeListOption,
	h *mixedversion.Helper,
	fn func() error,
) error {
	sort.Ints(nodes)
	var disabledNodes option.NodeListOption
	for _, node := range mvb.roachNodes {
		idx := sort.SearchInts(nodes, node)
		if idx == len(nodes) || nodes[idx] != node {
			disabledNodes = append(disabledNodes, node)
		}
	}

	if err := mvb.disableJobAdoption(ctx, l, disabledNodes, h); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	return mvb.enableJobAdoption(ctx, l, disabledNodes)
}

// createBackupCollection creates a new backup collection to be
// restored/verified at the end of the test. A full backup is created,
// and an incremental one is created on top of it. Both backups are
// created according to their respective `backupSpec`, indicating
// where they should be planned and executed.
func (mvb *mixedVersionBackup) createBackupCollection(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	fullBackupSpec backupSpec,
	incBackupSpec backupSpec,
	h *mixedversion.Helper,
) error {
	var collection backupCollection
	var timestamp string

	if err := mvb.runJobOnOneOf(ctx, l, fullBackupSpec.Execute.Nodes, h, func() error {
		var err error
		label := backupCollectionDesc(fullBackupSpec, incBackupSpec)
		collection, timestamp, err = mvb.runBackup(ctx, l, fullBackup{label}, rng, fullBackupSpec.Plan.Nodes, h)
		return err
	}); err != nil {
		return err
	}

	l.Printf("creating incremental backup for %s", collection.name)
	if err := mvb.runJobOnOneOf(ctx, l, incBackupSpec.Execute.Nodes, h, func() error {
		var err error
		collection, timestamp, err = mvb.runBackup(ctx, l, incrementalBackup{collection}, rng, incBackupSpec.Plan.Nodes, h)
		return err
	}); err != nil {
		return err
	}

	return mvb.saveFingerprints(ctx, l, rng, &collection, timestamp, h)
}

// sentinelFilePath returns the path to the file that prevents job
// adoption on the given node.
func (mvb *mixedVersionBackup) sentinelFilePath(
	ctx context.Context, l *logger.Logger, node int,
) (string, error) {
	result, err := mvb.cluster.RunWithDetailsSingleNode(
		ctx, l, mvb.cluster.Node(node), "echo -n {store-dir}",
	)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve store directory from node %d: %w", node, err)
	}
	return filepath.Join(result.Stdout, jobs.PreventAdoptionFile), nil
}

// disableJobAdoption disables job adoption on the given nodes by
// creating an empty file in `jobs.PreventAdoptionFile`. The function
// returns once any currently running jobs on the nodes terminate.
func (mvb *mixedVersionBackup) disableJobAdoption(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, h *mixedversion.Helper,
) error {
	l.Printf("disabling job adoption on nodes %v", nodes)
	eg, _ := errgroup.WithContext(ctx)
	for _, node := range nodes {
		node := node // capture range variable
		eg.Go(func() error {
			l.Printf("node %d: disabling job adoption", node)
			sentinelFilePath, err := mvb.sentinelFilePath(ctx, l, node)
			if err != nil {
				return err
			}
			if err := mvb.cluster.RunE(ctx, mvb.cluster.Node(node), "touch", sentinelFilePath); err != nil {
				return fmt.Errorf("node %d: failed to touch sentinel file %q: %w", node, sentinelFilePath, err)
			}

			// Wait for no jobs to be running on the node that we have halted
			// adoption on.
			l.Printf("node %d: waiting for all running jobs to terminate", node)
			if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
				db := h.Connect(node)
				var count int
				err := db.QueryRow(`SELECT count(*) FROM [SHOW JOBS] WHERE status = 'running'`).Scan(&count)
				if err != nil {
					l.Printf("node %d: error querying running jobs (%s)", node, err)
					return err
				}

				if count != 0 {
					l.Printf("node %d: %d running jobs...", node, count)
					return fmt.Errorf("node %d is still running %d jobs", node, count)
				}
				return nil
			}); err != nil {
				return err
			}

			l.Printf("node %d: job adoption disabled", node)
			return nil
		})
	}

	return eg.Wait()
}

// enableJobAdoption (re-)enables job adoption on the given nodes.
func (mvb *mixedVersionBackup) enableJobAdoption(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) error {
	l.Printf("enabling job adoption on nodes %v", nodes)
	eg, _ := errgroup.WithContext(ctx)
	for _, node := range nodes {
		node := node // capture range variable
		eg.Go(func() error {
			l.Printf("node %d: enabling job adoption", node)
			sentinelFilePath, err := mvb.sentinelFilePath(ctx, l, node)
			if err != nil {
				return err
			}

			if err := mvb.cluster.RunE(ctx, mvb.cluster.Node(node), "rm -f", sentinelFilePath); err != nil {
				return fmt.Errorf("node %d: failed to remove sentinel file %q: %w", node, sentinelFilePath, err)
			}

			l.Printf("node %d: job adoption enabled", node)
			return nil
		})
	}

	return eg.Wait()
}

// planAndRunBackups is the function that can be passed to the
// mixed-version test's `InMixedVersion` function. If the cluster is
// in mixed-binary state, four backup collections are created (see
// four cases in the function body). If all nodes are running the next
// version (which is different from the cluster version), then a
// backup collection is created in an arbitrary node.
func (mvb *mixedVersionBackup) planAndRunBackups(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	if len(mvb.collections) > 0 {
		return nil
	}
	tc := h.Context() // test context
	l.Printf("current context: %#v", tc)

	onPrevious := labeledNodes{
		Nodes: tc.FromVersionNodes, Version: sanitizeVersionForBackup(tc.FromVersion),
	}
	onNext := labeledNodes{
		Nodes: tc.ToVersionNodes, Version: sanitizeVersionForBackup(tc.ToVersion),
	}

	if len(tc.FromVersionNodes) > 0 {
		// Case 1: plan backups    -> previous node
		//         execute backups -> next node
		fullSpec := backupSpec{Plan: onPrevious, Execute: onNext}
		incSpec := fullSpec
		l.Printf("planning backup: %s", backupCollectionDesc(fullSpec, incSpec))
		if err := mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h); err != nil {
			return err
		}

		if true {
			return nil
		}

		// Case 2: plan backups   -> next node
		//         execute backups -> previous node
		fullSpec = backupSpec{Plan: onNext, Execute: onPrevious}
		incSpec = fullSpec
		l.Printf("planning backup: %s", backupCollectionDesc(fullSpec, incSpec))
		if err := mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h); err != nil {
			return err
		}

		// Case 3: plan & execute full backup        -> previous node
		//         plan & execute incremental backup -> next node
		fullSpec = backupSpec{Plan: onPrevious, Execute: onPrevious}
		incSpec = backupSpec{Plan: onNext, Execute: onNext}
		l.Printf("planning backup: %s", backupCollectionDesc(fullSpec, incSpec))
		if err := mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h); err != nil {
			return err
		}

		// Case 4: plan & execute full backup        -> next node
		//         plan & execute incremental backup -> previous node
		fullSpec = backupSpec{Plan: onNext, Execute: onNext}
		incSpec = backupSpec{Plan: onPrevious, Execute: onPrevious}
		l.Printf("planning backup: %s", backupCollectionDesc(fullSpec, incSpec))
		if err := mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h); err != nil {
			return err
		}
		return nil
	}

	l.Printf("all nodes running next version, running backup on arbitrary node")
	fullSpec := backupSpec{Plan: onNext, Execute: onNext}
	incSpec := fullSpec
	return mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h)
}

func (mvb *mixedVersionBackup) resetCluster(ctx context.Context, l *logger.Logger) error {
	mvb.stopWorkload()
	time.Sleep(3 * time.Second)

	if err := mvb.cluster.WipeE(ctx, l, mvb.roachNodes); err != nil {
		return fmt.Errorf("failed to wipe cluster: %w", err)
	}

	return clusterupgrade.StartWithBinary(
		ctx, l, mvb.cluster, mvb.roachNodes, mixedversion.CurrentCockroachPath, option.DefaultStartOptsNoBackups(),
	)
}

// verifyBackups cycles through all the backup collections created for
// the duration of the test, and verifies that restoring the backups
// in a new database results in the same data as when the backup was
// taken. This is accomplished by comparing the fingerprints after
// restoring with the expected fingerpring associated with the backup
// collection.
func (mvb *mixedVersionBackup) verifyBackups(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	l.Printf("verifying %d collections created during this test", len(mvb.collections))
	for _, collection := range mvb.collections {
		l.Printf("verifying %s", collection.name)
		uniqueName := fmt.Sprintf("restore_%s", invalidDBNameRE.ReplaceAllString(collection.name, "_"))

		// TODO: we are assuming this is a cluster backup here:
		if err := mvb.resetCluster(ctx, l); err != nil {
			return err
		}

		if _, isClusterBackup := collection.btype.(*clusterBackup); !isClusterBackup {
			// Create a database with the unique name generated. This will be
			// used by table and database backups.
			createDB := fmt.Sprintf("CREATE DATABASE %s", uniqueName)
			if err := h.Exec(rng, createDB); err != nil {
				return fmt.Errorf("backup %s: error creating database %s: %w", collection.name, uniqueName, err)
			}
		}
		restoreCmd, options := collection.btype.RestoreCommand(uniqueName)
		restoreOptions := append([]string{}, options...)
		// If the backup was created with an encryption passphrase, we
		// need to include it when restoring as well.
		for _, option := range collection.options {
			if ep, ok := option.(encryptionPassphrase); ok {
				restoreOptions = append(restoreOptions, ep.String())
			}
		}

		var optionsStr string
		if len(restoreOptions) > 0 {
			optionsStr = fmt.Sprintf(" WITH %s", strings.Join(restoreOptions, ", "))
		}
		restoreDB := fmt.Sprintf(
			"%s FROM LATEST IN '%s'%s",
			restoreCmd, collection.uri(), optionsStr,
		)
		if err := h.Exec(rng, restoreDB); err != nil {
			return fmt.Errorf("backup %s: error restoring: %w", collection.name, err)
		}

		origTables := collection.btype.TargetTables()
		// restoredTables := tableNamesWithDB(uniqueName, origTables) // TODO: bring this back for non-cluster
		restoredFingerprints, err := mvb.computeFingerprints(
			ctx, l, rng /* restoredTables */, origTables, "" /* timestamp */, h,
		)
		if err != nil {
			return fmt.Errorf("backup %s: error computing fingerprints: %w", collection.name, err)
		}

		loadDataForTable := func(table string) (string, error) {
			outQuery := fmt.Sprintf("SELECT * FROM %s", table)
			cmd := fmt.Sprintf("%s sql --insecure -e '%s'", mixedversion.CurrentCockroachPath, outQuery)
			node := mvb.roachNodes[0]
			out, err := mvb.cluster.RunWithDetailsSingleNode(ctx, l, option.NodeListOption{node}, cmd)
			if err != nil {
				return "", fmt.Errorf("failed to run query %q: %w", outQuery, err)
			}

			return out.Stdout, nil
		}

		if err := collection.checkRestoredFingerprints(l, restoredFingerprints, loadDataForTable); err != nil {
			return err
		}

		l.Printf("%s: OK", collection.name)
	}

	return nil
}

func registerBackupMixedVersion(r registry.Registry) {
	// backup/mixed-version tests different states of backup in a mixed
	// version cluster. The actual state of the cluster when a backup is
	// executed is randomized, so each run of the test will exercise a
	// different set of events. Reusing the same seed will produce the
	// same test.
	r.Add(registry.TestSpec{
		Name:              "backup/mixed-version",
		Owner:             registry.OwnerDisasterRecovery,
		Cluster:           r.MakeClusterSpec(5),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Timeout:           4 * time.Hour,
		RequiresLicense:   true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.IsLocal() && runtime.GOARCH == "arm64" {
				t.Skip("Skip under ARM64. See https://github.com/cockroachdb/cockroach/issues/89268")
			}

			roachNodes := c.Range(1, c.Spec().NodeCount-1)
			workloadNode := c.Node(c.Spec().NodeCount)

			uploadVersion(ctx, t, c, workloadNode, clusterupgrade.MainVersion)
			bankRun := roachtestutil.NewCommand("./cockroach workload run bank").
				Arg("{pgurl%s}", roachNodes).
				Option("tolerate-errors")

			mvt := mixedversion.NewTest(ctx, t, t.L(), c, roachNodes)
			mvb := newMixedVersionBackup(c, roachNodes, "bank")
			mvt.OnStartup("set short job interval", mvb.setShortJobIntervals)
			mvt.OnStartup("load backup data", mvb.loadBackupData)
			mvb.stopWorkload = mvt.Workload("bank", workloadNode, nil /* initCmd */, bankRun)

			mvt.InMixedVersion("plan and run backups", mvb.planAndRunBackups)
			mvt.AfterUpgradeFinalized("verify backups", mvb.verifyBackups)

			mvt.Run()
		},
	})
}
