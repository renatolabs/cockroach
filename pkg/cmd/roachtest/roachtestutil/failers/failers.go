// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package failers

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// FailureMode specifies a failure mode.
type FailureMode string

const (
	FailureModeBlackhole     FailureMode = "blackhole"
	FailureModeBlackholeRecv FailureMode = "blackhole-recv"
	FailureModeBlackholeSend FailureMode = "blackhole-send"
	FailureModeCrash         FailureMode = "crash"
	FailureModeDeadlock      FailureMode = "deadlock"
	FailureModeDiskStall     FailureMode = "disk-stall"
	FailureModePause         FailureMode = "pause"
	FailureModeNoop          FailureMode = "noop"
)

var AllFailureModes = []FailureMode{
	FailureModeBlackhole,
	FailureModeBlackholeRecv,
	FailureModeBlackholeSend,
	FailureModeCrash,
	FailureModeDeadlock,
	FailureModeDiskStall,
	FailureModePause,
	// FailureModeNoop intentionally omitted
}

// MakeFailer creates a new failer for the given failureMode. It may return a
// noopFailer on local clusters.
func MakeFailer(
	t test.Test,
	c cluster.Cluster,
	m cluster.Monitor,
	failureMode FailureMode,
	opts option.StartOpts,
	settings install.ClusterSettings,
	rng *rand.Rand,
) Failer {
	f := MakeFailerWithoutLocalNoop(t, c, m, failureMode, opts, settings, rng)
	if c.IsLocal() && !f.CanUseLocal() {
		t.L().Printf(
			`failure mode %q not supported on local clusters, using "noop" failure mode instead`,
			failureMode)
		f = &noopFailer{}
	}
	return f
}

func MakeFailerWithoutLocalNoop(
	t test.Test,
	c cluster.Cluster,
	m cluster.Monitor,
	failureMode FailureMode,
	opts option.StartOpts,
	settings install.ClusterSettings,
	rng *rand.Rand,
) Failer {
	switch failureMode {
	case FailureModeBlackhole:
		return &BlackholeFailer{
			t:      t,
			c:      c,
			input:  true,
			output: true,
		}
	case FailureModeBlackholeRecv:
		return &BlackholeFailer{
			t:     t,
			c:     c,
			input: true,
		}
	case FailureModeBlackholeSend:
		return &BlackholeFailer{
			t:      t,
			c:      c,
			output: true,
		}
	case FailureModeCrash:
		return &crashFailer{
			t:             t,
			c:             c,
			m:             m,
			startOpts:     opts,
			startSettings: settings,
		}
	case FailureModeDeadlock:
		return &DeadlockFailer{
			t:                t,
			c:                c,
			m:                m,
			rng:              rng,
			startOpts:        opts,
			startSettings:    settings,
			OnlyLeaseholders: true,
			NumReplicas:      5,
		}
	case FailureModeDiskStall:
		return &diskStallFailer{
			t:             t,
			c:             c,
			m:             m,
			startOpts:     opts,
			startSettings: settings,
			staller:       &DMSetupDiskStaller{t: t, c: c},
		}
	case FailureModePause:
		return &pauseFailer{
			t: t,
			c: c,
		}
	case FailureModeNoop:
		return &noopFailer{}
	default:
		t.Fatalf("unknown failure mode %s", failureMode)
		return nil
	}
}

// Failer fails and recovers a given node in some particular way.
type Failer interface {
	fmt.Stringer

	// Mode returns the failure mode of the failer.
	Mode() FailureMode

	// CanUseLocal returns true if the failer can be run with a local cluster.
	CanUseLocal() bool

	// CanRunWith returns true if the failer can run concurrently with another
	// given failure mode on a different cluster node. It is not required to
	// commute, i.e. A may not be able to run with B even though B can run with A.
	CanRunWith(other FailureMode) bool

	// Setup prepares the failer. It is called before the cluster is started.
	Setup(ctx context.Context)

	// Cleanup cleans up when the test exits. This is needed e.g. when the cluster
	// is reused by a different test.
	Cleanup(ctx context.Context)

	// Ready is called before failing each node, when the cluster and workload is
	// running and after recovering the previous node failure if any.
	Ready(ctx context.Context, nodeID int)

	// Fail fails the given node.
	Fail(ctx context.Context, nodeID int)

	// Recover recovers the given node.
	Recover(ctx context.Context, nodeID int)
}

// PartialFailer supports partial failures between specific node pairs.
type PartialFailer interface {
	Failer

	// FailPartial fails the node for the given peers.
	FailPartial(ctx context.Context, nodeID int, peerIDs []int)
}

// noopFailer doesn't do anything.
type noopFailer struct{}

func (f *noopFailer) Mode() FailureMode                       { return FailureModeNoop }
func (f *noopFailer) String() string                          { return string(f.Mode()) }
func (f *noopFailer) CanUseLocal() bool                       { return true }
func (f *noopFailer) CanRunWith(FailureMode) bool             { return true }
func (f *noopFailer) Setup(context.Context)                   {}
func (f *noopFailer) Ready(context.Context, int)              {}
func (f *noopFailer) Cleanup(context.Context)                 {}
func (f *noopFailer) Fail(context.Context, int)               {}
func (f *noopFailer) FailPartial(context.Context, int, []int) {}
func (f *noopFailer) Recover(context.Context, int)            {}

// BlackholeFailer causes a network failure where TCP/IP packets to/from port
// 26257 are dropped, causing network hangs and timeouts.
//
// If only one if input or output are enabled, connections in that direction
// will fail (even already established connections), but connections in the
// other direction are still functional (including responses).
type BlackholeFailer struct {
	t      test.Test
	c      cluster.Cluster
	input  bool
	output bool
}

func NewBlackholeFailer(t test.Test, c cluster.Cluster, input, output bool) *BlackholeFailer {
	return &BlackholeFailer{t, c, input, output}
}

func (f *BlackholeFailer) Mode() FailureMode {
	if f.input && !f.output {
		return FailureModeBlackholeRecv
	} else if f.output && !f.input {
		return FailureModeBlackholeSend
	}
	return FailureModeBlackhole
}

func (f *BlackholeFailer) String() string              { return string(f.Mode()) }
func (f *BlackholeFailer) CanUseLocal() bool           { return false } // needs iptables
func (f *BlackholeFailer) CanRunWith(FailureMode) bool { return true }
func (f *BlackholeFailer) Setup(context.Context)       {}
func (f *BlackholeFailer) Ready(context.Context, int)  {}

func (f *BlackholeFailer) Cleanup(ctx context.Context) {
	f.c.Run(ctx, option.WithNodes(f.c.All()), `sudo iptables -F`)
}

func (f *BlackholeFailer) Fail(ctx context.Context, nodeID int) {
	pgport := fmt.Sprintf("{pgport:%d}", nodeID)

	// When dropping both input and output, make sure we drop packets in both
	// directions for both the inbound and outbound TCP connections, such that we
	// get a proper black hole. Only dropping one direction for both of INPUT and
	// OUTPUT will still let e.g. TCP retransmits through, which may affect the
	// TCP stack behavior and is not representative of real network outages.
	//
	// For the asymmetric partitions, only drop packets in one direction since
	// this is representative of accidental firewall rules we've seen cause such
	// outages in the wild.
	if f.input && f.output {
		// Inbound TCP connections, both received and sent packets.
		f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(`sudo iptables -A INPUT -p tcp --dport %s -j DROP`, pgport))
		f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(`sudo iptables -A OUTPUT -p tcp --sport %s -j DROP`, pgport))
		// Outbound TCP connections, both sent and received packets.
		f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(`sudo iptables -A OUTPUT -p tcp --dport %s -j DROP`, pgport))
		f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(`sudo iptables -A INPUT -p tcp --sport %s -j DROP`, pgport))
	} else if f.input {
		f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(`sudo iptables -A INPUT -p tcp --dport %s -j DROP`, pgport))
	} else if f.output {
		f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(`sudo iptables -A OUTPUT -p tcp --dport %s -j DROP`, pgport))
	}
}

// FailPartial creates a partial blackhole failure between the given node and
// peers.
func (f *BlackholeFailer) FailPartial(ctx context.Context, nodeID int, peerIDs []int) {
	peerIPs, err := f.c.InternalIP(ctx, f.t.L(), peerIDs)
	require.NoError(f.t, err)

	for _, peerIP := range peerIPs {
		pgport := fmt.Sprintf("{pgport:%d}", nodeID)

		// When dropping both input and output, make sure we drop packets in both
		// directions for both the inbound and outbound TCP connections, such that
		// we get a proper black hole. Only dropping one direction for both of INPUT
		// and OUTPUT will still let e.g. TCP retransmits through, which may affect
		// TCP stack behavior and is not representative of real network outages.
		//
		// For the asymmetric partitions, only drop packets in one direction since
		// this is representative of accidental firewall rules we've seen cause such
		// outages in the wild.
		if f.input && f.output {
			// Inbound TCP connections, both received and sent packets.
			f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(
				`sudo iptables -A INPUT -p tcp -s %s --dport %s -j DROP`, peerIP, pgport))
			f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(
				`sudo iptables -A OUTPUT -p tcp -d %s --sport %s -j DROP`, peerIP, pgport))
			// Outbound TCP connections, both sent and received packets.
			f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(
				`sudo iptables -A OUTPUT -p tcp -d %s --dport %s -j DROP`, peerIP, pgport))
			f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(
				`sudo iptables -A INPUT -p tcp -s %s --sport %s -j DROP`, peerIP, pgport))
		} else if f.input {
			f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(
				`sudo iptables -A INPUT -p tcp -s %s --dport %s -j DROP`, peerIP, pgport))
		} else if f.output {
			f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), fmt.Sprintf(
				`sudo iptables -A OUTPUT -p tcp -d %s --dport %s -j DROP`, peerIP, pgport))
		}
	}
}

func (f *BlackholeFailer) Recover(ctx context.Context, nodeID int) {
	f.c.Run(ctx, option.WithNodes(f.c.Node(nodeID)), `sudo iptables -F`)
}

// crashFailer is a process crash where the TCP/IP stack remains responsive
// and sends immediate RST packets to peers.
type crashFailer struct {
	t             test.Test
	c             cluster.Cluster
	m             cluster.Monitor
	startOpts     option.StartOpts
	startSettings install.ClusterSettings
}

func (f *crashFailer) Mode() FailureMode           { return FailureModeCrash }
func (f *crashFailer) String() string              { return string(f.Mode()) }
func (f *crashFailer) CanUseLocal() bool           { return true }
func (f *crashFailer) CanRunWith(FailureMode) bool { return true }
func (f *crashFailer) Setup(context.Context)       {}
func (f *crashFailer) Ready(context.Context, int)  {}
func (f *crashFailer) Cleanup(context.Context)     {}

func (f *crashFailer) Fail(ctx context.Context, nodeID int) {
	f.m.ExpectDeath()
	f.c.Stop(ctx, f.t.L(), option.DefaultStopOpts(), f.c.Node(nodeID)) // uses SIGKILL
}

func (f *crashFailer) Recover(ctx context.Context, nodeID int) {
	f.c.Start(ctx, f.t.L(), f.startOpts, f.startSettings, f.c.Node(nodeID))
}

// DeadlockFailer deadlocks replicas. In addition to deadlocks, this failure
// mode is representative of all failure modes that leave a replica unresponsive
// while the node is otherwise still functional.
type DeadlockFailer struct {
	t                test.Test
	c                cluster.Cluster
	m                cluster.Monitor
	rng              *rand.Rand
	startOpts        option.StartOpts
	startSettings    install.ClusterSettings
	OnlyLeaseholders bool
	NumReplicas      int

	locks  map[int][]roachpb.RangeID // track locks by node
	ranges map[int][]roachpb.RangeID // ranges present on nodes
	leases map[int][]roachpb.RangeID // range leases present on nodes
}

func (f *DeadlockFailer) Mode() FailureMode             { return FailureModeDeadlock }
func (f *DeadlockFailer) String() string                { return string(f.Mode()) }
func (f *DeadlockFailer) CanUseLocal() bool             { return true }
func (f *DeadlockFailer) CanRunWith(m FailureMode) bool { return true }
func (f *DeadlockFailer) Setup(context.Context)         {}
func (f *DeadlockFailer) Cleanup(context.Context)       {}

func (f *DeadlockFailer) Ready(ctx context.Context, nodeID int) {
	// In chaos tests, other nodes will be failing concurrently. We therefore
	// can't run SHOW CLUSTER RANGES WITH DETAILS in Fail(), since it needs to
	// read from all ranges. Instead, we fetch a snapshot of replicas and leases
	// now, and if any replicas should move we'll skip them later.
	//
	// We also have to ensure we have an active connection to the node in the
	// pool, since we may be unable to create one during concurrent failures.
	conn := f.c.Conn(ctx, f.t.L(), nodeID)
	rows, err := conn.QueryContext(ctx,
		`SELECT range_id, replicas, lease_holder FROM [SHOW CLUSTER RANGES WITH DETAILS]`)
	require.NoError(f.t, err)

	f.ranges = map[int][]roachpb.RangeID{}
	f.leases = map[int][]roachpb.RangeID{}
	for rows.Next() {
		var rangeID roachpb.RangeID
		var replicas []int64
		var leaseHolder int
		require.NoError(f.t, rows.Scan(&rangeID, (*pq.Int64Array)(&replicas), &leaseHolder))
		f.leases[leaseHolder] = append(f.leases[leaseHolder], rangeID)
		for _, nodeID := range replicas {
			f.ranges[int(nodeID)] = append(f.ranges[int(nodeID)], rangeID)
		}
	}
	require.NoError(f.t, rows.Err())
}

func (f *DeadlockFailer) Fail(ctx context.Context, nodeID int) {
	require.NotZero(f.t, f.NumReplicas)
	if f.locks == nil {
		f.locks = map[int][]roachpb.RangeID{}
	}

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second) // can take a while to lock
	defer cancel()

	var ranges []roachpb.RangeID
	if f.OnlyLeaseholders {
		ranges = append(ranges, f.leases[nodeID]...)
	} else {
		ranges = append(ranges, f.ranges[nodeID]...)
	}
	f.rng.Shuffle(len(ranges), func(i, j int) {
		ranges[i], ranges[j] = ranges[j], ranges[i]
	})

	conn := f.c.Conn(ctx, f.t.L(), nodeID)

	for i := 0; i < len(ranges) && len(f.locks[nodeID]) < f.NumReplicas; i++ {
		rangeID := ranges[i]
		var locked bool
		require.NoError(f.t, conn.QueryRowContext(ctx,
			`SELECT crdb_internal.unsafe_lock_replica($1::int, true)`, rangeID).Scan(&locked))
		if locked {
			f.locks[nodeID] = append(f.locks[nodeID], rangeID)
			f.t.L().Printf("locked r%d on n%d", rangeID, nodeID)
		}
	}
	// Some nodes may have fewer ranges than the requested numReplicas locks, and
	// replicas may also have moved in the meanwhile. Just assert that we were able
	// to lock at least 1 replica.
	require.NotEmpty(f.t, f.locks[nodeID], "didn't lock any replicas")
}

func (f *DeadlockFailer) Recover(ctx context.Context, nodeID int) {
	if f.locks == nil || len(f.locks[nodeID]) == 0 {
		return
	}

	err := func() error {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		conn, err := f.c.ConnE(ctx, f.t.L(), nodeID)
		if err != nil {
			return err
		}
		for _, rangeID := range f.locks[nodeID] {
			var unlocked bool
			err := conn.QueryRowContext(ctx,
				`SELECT crdb_internal.unsafe_lock_replica($1, false)`, rangeID).Scan(&unlocked)
			if err != nil {
				return err
			} else if !unlocked {
				return errors.Errorf("r%d was not unlocked", rangeID)
			} else {
				f.t.L().Printf("unlocked r%d on n%d", rangeID, nodeID)
			}
		}
		return nil
	}()
	// We may have locked replicas that prevent us from connecting to the node
	// again, so we fall back to restarting the node.
	if err != nil {
		f.t.L().Printf("failed to unlock replicas on n%d, restarting node: %s", nodeID, err)
		f.m.ExpectDeath()
		f.c.Stop(ctx, f.t.L(), option.DefaultStopOpts(), f.c.Node(nodeID))
		f.c.Start(ctx, f.t.L(), f.startOpts, f.startSettings, f.c.Node(nodeID))
	}
	delete(f.locks, nodeID)
}

// diskStallFailer stalls the disk indefinitely. This should cause the node to
// eventually self-terminate, but we'd want leases to move off before then.
type diskStallFailer struct {
	t             test.Test
	c             cluster.Cluster
	m             cluster.Monitor
	startOpts     option.StartOpts
	startSettings install.ClusterSettings
	staller       DiskStaller
}

func (f *diskStallFailer) Mode() FailureMode           { return FailureModeDiskStall }
func (f *diskStallFailer) String() string              { return string(f.Mode()) }
func (f *diskStallFailer) CanUseLocal() bool           { return false } // needs dmsetup
func (f *diskStallFailer) CanRunWith(FailureMode) bool { return true }

func (f *diskStallFailer) Setup(ctx context.Context) {
	f.staller.Setup(ctx)
}

func (f *diskStallFailer) Cleanup(ctx context.Context) {
	f.staller.Unstall(ctx, f.c.All())
	// We have to stop the cluster before cleaning up the staller.
	f.m.ExpectDeaths(int32(f.c.Spec().NodeCount))
	f.c.Stop(ctx, f.t.L(), option.DefaultStopOpts(), f.c.All())
	f.staller.Cleanup(ctx)
}

func (f *diskStallFailer) Ready(ctx context.Context, nodeID int) {
	// Other failure modes may have disabled the disk stall detector (see
	// pauseFailer), so we explicitly enable it.
	conn := f.c.Conn(ctx, f.t.L(), nodeID)
	_, err := conn.ExecContext(ctx,
		`SET CLUSTER SETTING storage.max_sync_duration.fatal.enabled = true`)
	require.NoError(f.t, err)
}

func (f *diskStallFailer) Fail(ctx context.Context, nodeID int) {
	// Pebble's disk stall detector should crash the node.
	f.m.ExpectDeath()
	f.staller.Stall(ctx, f.c.Node(nodeID))
}

func (f *diskStallFailer) Recover(ctx context.Context, nodeID int) {
	f.staller.Unstall(ctx, f.c.Node(nodeID))
	// Pebble's disk stall detector should have terminated the node, but in case
	// it didn't, we explicitly stop it first.
	f.c.Stop(ctx, f.t.L(), option.DefaultStopOpts(), f.c.Node(nodeID))
	f.c.Start(ctx, f.t.L(), f.startOpts, f.startSettings, f.c.Node(nodeID))
}

// pauseFailer pauses the process, but keeps the OS (and thus network
// connections) alive.
type pauseFailer struct {
	t test.Test
	c cluster.Cluster
}

func (f *pauseFailer) Mode() FailureMode       { return FailureModePause }
func (f *pauseFailer) String() string          { return string(f.Mode()) }
func (f *pauseFailer) CanUseLocal() bool       { return true }
func (f *pauseFailer) Setup(context.Context)   {}
func (f *pauseFailer) Cleanup(context.Context) {}

func (f *pauseFailer) CanRunWith(other FailureMode) bool {
	// Since we disable the disk stall detector, we can't run concurrently with
	// a disk stall on a different node.
	return other != FailureModeDiskStall
}

func (f *pauseFailer) Ready(ctx context.Context, nodeID int) {
	// The process pause can trip the disk stall detector, so we disable it. We
	// could let it fire, but we'd like to see if the node can recover from the
	// pause and keep working. It will be re-enabled by diskStallFailer.Ready().
	conn := f.c.Conn(ctx, f.t.L(), nodeID)
	_, err := conn.ExecContext(ctx,
		`SET CLUSTER SETTING storage.max_sync_duration.fatal.enabled = false`)
	require.NoError(f.t, err)
}

func (f *pauseFailer) Fail(ctx context.Context, nodeID int) {
	f.c.Signal(ctx, f.t.L(), 19, f.c.Node(nodeID)) // SIGSTOP
}

func (f *pauseFailer) Recover(ctx context.Context, nodeID int) {
	f.c.Signal(ctx, f.t.L(), 18, f.c.Node(nodeID)) // SIGCONT
	// NB: We don't re-enable the disk stall detector here, but instead rely on
	// diskStallFailer.Ready to ensure it's enabled, since the cluster likely
	// hasn't recovered yet and we may fail to set the cluster setting.
}

type DiskStaller interface {
	Setup(ctx context.Context)
	Cleanup(ctx context.Context)
	Stall(ctx context.Context, nodes option.NodeListOption)
	Unstall(ctx context.Context, nodes option.NodeListOption)
	DataDir() string
	LogDir() string
}

type DMSetupDiskStaller struct {
	t test.Test
	c cluster.Cluster
}

func NewDMSetupDiskStaller(t test.Test, c cluster.Cluster) *DMSetupDiskStaller {
	return &DMSetupDiskStaller{t, c}
}

var _ DiskStaller = (*DMSetupDiskStaller)(nil)

func (s *DMSetupDiskStaller) device() string { return getDevice(s.t, s.c) }

func (s *DMSetupDiskStaller) Setup(ctx context.Context) {
	dev := s.device()
	// snapd will run "snapd auto-import /dev/dm-0" via udev triggers when
	// /dev/dm-0 is created. This possibly interferes with the dmsetup create
	// reload, so uninstall snapd.
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo apt-get purge -y snapd`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo umount -f /mnt/data1 || true`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo dmsetup remove_all`)
	err := s.c.RunE(ctx, option.WithNodes(s.c.All()), `echo "0 $(sudo blockdev --getsz `+dev+`) linear `+dev+` 0" | `+
		`sudo dmsetup create data1`)
	if err != nil {
		// This has occasionally been seen to fail with "Device or resource busy",
		// with no clear explanation. Try to find out who it is.
		s.c.Run(ctx, option.WithNodes(s.c.All()), "sudo bash -c 'ps aux; dmsetup status; mount; lsof'")
		s.t.Fatal(err)
	}
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo mount /dev/mapper/data1 /mnt/data1`)
}

func (s *DMSetupDiskStaller) Cleanup(ctx context.Context) {
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo dmsetup resume data1`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo umount /mnt/data1`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo dmsetup remove_all`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo mount /mnt/data1`)
	// Reinstall snapd in case subsequent tests need it.
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo apt-get install -y snapd`)
}

func (s *DMSetupDiskStaller) Stall(ctx context.Context, nodes option.NodeListOption) {
	s.c.Run(ctx, option.WithNodes(nodes), `sudo dmsetup suspend --noflush --nolockfs data1`)
}

func (s *DMSetupDiskStaller) Unstall(ctx context.Context, nodes option.NodeListOption) {
	s.c.Run(ctx, option.WithNodes(nodes), `sudo dmsetup resume data1`)
}

func (s *DMSetupDiskStaller) DataDir() string { return "{store-dir}" }
func (s *DMSetupDiskStaller) LogDir() string  { return "logs" }

type CGroupDiskStaller struct {
	t           test.Test
	c           cluster.Cluster
	readOrWrite []BandwidthReadWrite
	logsToo     bool
}

func NewCGroupDiskStaller(
	t test.Test, c cluster.Cluster, brw []BandwidthReadWrite, logsToo bool,
) *CGroupDiskStaller {
	return &CGroupDiskStaller{t, c, brw, logsToo}
}

var _ DiskStaller = (*CGroupDiskStaller)(nil)

func (s *CGroupDiskStaller) DataDir() string { return "{store-dir}" }
func (s *CGroupDiskStaller) LogDir() string {
	return "logs"
}
func (s *CGroupDiskStaller) Setup(ctx context.Context) {
	if s.logsToo {
		s.c.Run(ctx, option.WithNodes(s.c.All()), "mkdir -p {store-dir}/logs")
		s.c.Run(ctx, option.WithNodes(s.c.All()), "rm -f logs && ln -s {store-dir}/logs logs || true")
	}
}
func (s *CGroupDiskStaller) Cleanup(ctx context.Context) {}

func (s *CGroupDiskStaller) Stall(ctx context.Context, nodes option.NodeListOption) {
	// Shuffle the order of read and write stall initiation.
	rand.Shuffle(len(s.readOrWrite), func(i, j int) {
		s.readOrWrite[i], s.readOrWrite[j] = s.readOrWrite[j], s.readOrWrite[i]
	})
	for _, rw := range s.readOrWrite {
		// NB: I don't understand why, but attempting to set a
		// bytesPerSecond={0,1} results in Invalid argument from the io.max
		// cgroupv2 API.
		if err := s.setThroughput(ctx, nodes, rw, throughput{limited: true, bytesPerSecond: 4}); err != nil {
			s.t.Fatal(err)
		}
	}
}

func (s *CGroupDiskStaller) Unstall(ctx context.Context, nodes option.NodeListOption) {
	for _, rw := range s.readOrWrite {
		err := s.setThroughput(ctx, nodes, rw, throughput{limited: false})
		s.t.L().PrintfCtx(ctx, "error unstalling the disk; stumbling on: %v", err)
		// NB: We log the error and continue on because unstalling may not
		// succeed if the process has successfully exited.
	}
}

func (s *CGroupDiskStaller) device() (major, minor int) {
	// TODO(jackson): Programmatically determine the device major,minor numbers.
	// eg,:
	//    deviceName := getDevice(s.t, s.c)
	//    `cat /proc/partitions` and find `deviceName`
	switch s.c.Cloud() {
	case spec.GCE:
		// ls -l /dev/sdb
		// brw-rw---- 1 root disk 8, 16 Mar 27 22:08 /dev/sdb
		return 8, 16
	default:
		s.t.Fatalf("unsupported cloud %q", s.c.Cloud())
		return 0, 0
	}
}

type throughput struct {
	limited        bool
	bytesPerSecond int
}

type BandwidthReadWrite int8

const (
	ReadBandwidth BandwidthReadWrite = iota
	WriteBandwidth
)

func (rw BandwidthReadWrite) cgroupV2BandwidthProp() string {
	switch rw {
	case ReadBandwidth:
		return "rbps"
	case WriteBandwidth:
		return "wbps"
	default:
		panic("unreachable")
	}
}

func (s *CGroupDiskStaller) setThroughput(
	ctx context.Context, nodes option.NodeListOption, rw BandwidthReadWrite, bw throughput,
) error {
	maj, min := s.device()
	cockroachIOController := filepath.Join("/sys/fs/cgroup/system.slice", roachtestutil.SystemInterfaceSystemdUnitName()+".service", "io.max")

	bytesPerSecondStr := "max"
	if bw.limited {
		bytesPerSecondStr = fmt.Sprintf("%d", bw.bytesPerSecond)
	}
	return s.c.RunE(ctx, option.WithNodes(nodes), "sudo", "/bin/bash", "-c", fmt.Sprintf(
		`'echo %d:%d %s=%s > %s'`,
		maj,
		min,
		rw.cgroupV2BandwidthProp(),
		bytesPerSecondStr,
		cockroachIOController,
	))
}

func getDevice(t test.Test, c cluster.Cluster) string {
	switch c.Cloud() {
	case spec.GCE:
		return "/dev/sdb"
	case spec.AWS:
		return "/dev/nvme1n1"
	default:
		t.Fatalf("unsupported cloud %q", c.Cloud())
		return ""
	}
}
