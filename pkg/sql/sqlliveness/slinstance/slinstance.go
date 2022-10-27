// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package slinstance provides functionality for acquiring sqlliveness leases
// via sessions that have a unique id and expiration. The creation and
// maintenance of session liveness is entrusted to the Instance. Each SQL
// server will have a handle to an instance.
package slinstance

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/logtags"
)

var (
	// DefaultTTL specifies the time to expiration when a session is created.
	DefaultTTL = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.sqlliveness.ttl",
		"default sqlliveness session ttl",
		40*time.Second,
		settings.NonNegativeDuration,
	)
	// DefaultHeartBeat specifies the period between attempts to extend a session.
	DefaultHeartBeat = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.sqlliveness.heartbeat",
		"duration heart beats to push session expiration further out in time",
		5*time.Second,
		settings.NonNegativeDuration,
	)
)

// Writer provides interactions with the storage of session records.
type Writer interface {
	// Insert stores the input Session.
	Insert(ctx context.Context, id sqlliveness.SessionID, expiration hlc.Timestamp) error
	// Update looks for a Session with the same SessionID as the input Session in
	// the storage and if found replaces it with the input returning true.
	// Otherwise it returns false to indicate that the session does not exist.
	Update(ctx context.Context, id sqlliveness.SessionID, expiration hlc.Timestamp) (bool, error)
}

type session struct {
	id    sqlliveness.SessionID
	start hlc.Timestamp

	mu struct {
		syncutil.RWMutex
		exp hlc.Timestamp
	}
}

// ID implements the sqlliveness.Session interface.
func (s *session) ID() sqlliveness.SessionID { return s.id }

// Expiration implements the sqlliveness.Session interface.
func (s *session) Expiration() hlc.Timestamp {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.exp
}

// Start implements the sqlliveness.Session interface.
func (s *session) Start() hlc.Timestamp {
	return s.start
}

func (s *session) setExpiration(exp hlc.Timestamp) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.exp = exp
}

// SessionEventListener is an interface used by the Instance to notify
// listeners of some state changes.
type SessionEventListener interface {
	// OnSessionDeleted is called when the session liveness record is found to be
	// missing.
	OnSessionDeleted(context.Context)
}

// Instance implements the sqlliveness.Instance interface by storing the
// liveness sessions in table system.sqlliveness and relying on a heart beat
// loop to extend the existing sessions' expirations or creating a new session
// to replace a session that has expired and deleted from the table.
// TODO(rima): Rename Instance to avoid confusion with sqlinstance.SQLInstance.
type Instance struct {
	clock    *hlc.Clock
	settings *cluster.Settings
	stopper  *stop.Stopper
	// sessionEvents gets notified of some session state changes.
	sessionEvents SessionEventListener
	storage       Writer
	ttl           func() time.Duration
	hb            func() time.Duration
	testKnobs     sqlliveness.TestingKnobs
	mu            struct {
		syncutil.Mutex
		startErr error
		blockCh  chan struct{}
		s        *session
	}
}

func (l *Instance) getSessionOrBlockCh() (*session, chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mu.s != nil {
		return l.mu.s, nil
	}
	return nil, l.mu.blockCh
}

func (l *Instance) setSession(s *session) {
	l.mu.Lock()
	l.mu.s = s
	// Notify all calls to Session that a non-nil session is available.
	close(l.mu.blockCh)
	l.mu.Unlock()
}

func (l *Instance) clearSession(ctx context.Context) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if expiration := l.mu.s.Expiration(); expiration.Less(l.clock.Now()) {
		l.sessionEvents.OnSessionDeleted(ctx)
	}
	l.mu.s = nil
	l.mu.blockCh = make(chan struct{})
}

// createSession tries until it can create a new session and returns an error
// only if the heart beat loop should exit.
func (l *Instance) createSession(ctx context.Context) (*session, error) {
	id := sqlliveness.SessionID(uuid.MakeV4().GetBytes())
	start := l.clock.Now()
	exp := start.Add(l.ttl().Nanoseconds(), 0)
	s := &session{
		id:    id,
		start: start,
	}
	s.mu.exp = exp

	opts := retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     1.5,
	}
	everySecond := log.Every(time.Second)
	var err error
	for i, r := 0, retry.StartWithCtx(ctx, opts); r.Next(); {
		i++
		if err = l.storage.Insert(ctx, s.id, s.Expiration()); err != nil {
			if ctx.Err() != nil {
				break
			}
			if everySecond.ShouldLog() {
				log.Errorf(ctx, "failed to create a session at %d-th attempt: %v", i, err)
			}
			// Unauthenticated errors are unrecoverable, we should break instead
			// of retrying.
			if grpcutil.IsAuthError(err) {
				break
			}
			continue
		}
		break
	}
	if err != nil {
		return nil, err
	}
	log.Infof(ctx, "created new SQL liveness session %s", s.ID())
	return s, nil
}

// extendSession updates the session record with a new expiration time.
//
// Returns false if the session record is not found, meaning that someone else
// deleted it.
//
// extendSession keeps retrying on error until ctx is canceled. Thus, an error
// is only ever returned when the ctx is canceled.
func (l *Instance) extendSession(ctx context.Context, s *session) (bool, error) {
	exp := l.clock.Now().Add(l.ttl().Nanoseconds(), 0)

	opts := retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     1.5,
	}
	var err error
	var found bool
	// Retry until success or until the context is canceled.
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		if found, err = l.storage.Update(ctx, s.ID(), exp); err != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}
		break
	}
	if err != nil {
		if ctx.Err() == nil {
			log.Fatalf(ctx, "expected canceled ctx on err: %s", err)
		}
		return false, err
	}

	if !found {
		return false, nil
	}

	s.setExpiration(exp)
	return true, nil
}

func (l *Instance) heartbeatLoop(ctx context.Context) {
	defer func() {
		log.Warning(ctx, "exiting heartbeat loop")
	}()
	// Operations below retry endlessly after the stopper started quiescing if we
	// don't cancel their ctx.
	ctx, cancel := l.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()
	t := timeutil.NewTimer()
	defer t.Stop()

	t.Reset(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			t.Read = true
			s, _ := l.getSessionOrBlockCh()
			if s == nil {
				newSession, err := l.createSession(ctx)
				if err != nil {
					func() {
						l.mu.Lock()
						defer l.mu.Unlock()
						l.mu.startErr = err
						// There was an unrecoverable error when trying to
						// create the session. Notify all calls to Session that
						// the session failed.
						close(l.mu.blockCh)
					}()
					l.sessionEvents.OnSessionDeleted(ctx)
					return
				}
				l.setSession(newSession)
				t.Reset(l.hb())
				continue
			}
			found, err := l.extendSession(ctx, s)
			if err != nil {
				l.clearSession(ctx)
				return
			}
			if !found {
				l.clearSession(ctx)
				// Start next loop iteration immediately to insert a new session.
				t.Reset(0)
				continue
			}
			if log.V(2) {
				log.Infof(ctx, "extended SQL liveness session %s", s.ID())
			}
			t.Reset(l.hb())
		}
	}
}

// NewSQLInstance returns a new Instance struct and starts its heartbeating
// loop.
//
// sessionEvents, if not nil, gets notified of some session state transitions.
func NewSQLInstance(
	stopper *stop.Stopper,
	clock *hlc.Clock,
	storage Writer,
	settings *cluster.Settings,
	testKnobs *sqlliveness.TestingKnobs,
	sessionEvents SessionEventListener,
) *Instance {
	if sessionEvents == nil {
		sessionEvents = &dummySessionEventListener{}
	}

	l := &Instance{
		clock:         clock,
		settings:      settings,
		storage:       storage,
		stopper:       stopper,
		sessionEvents: sessionEvents,
		ttl: func() time.Duration {
			return DefaultTTL.Get(&settings.SV)
		},
		hb: func() time.Duration {
			return DefaultHeartBeat.Get(&settings.SV)
		},
	}
	if testKnobs != nil {
		l.testKnobs = *testKnobs
	}
	l.mu.blockCh = make(chan struct{})
	return l
}

// Start runs the hearbeat loop.
func (l *Instance) Start(ctx context.Context) {
	l.mu.Lock()
	defer l.mu.Unlock()
	log.Infof(ctx, "starting SQL liveness instance")
	// Detach from ctx's cancelation.
	taskCtx := logtags.WithTags(context.Background(), logtags.FromContext(ctx))
	_ = l.stopper.RunAsyncTask(taskCtx, "slinstance", l.heartbeatLoop)
}

// Session returns a live session id. For each Sqlliveness instance the
// invariant is that there exists at most one live session at any point in time.
// If the current one has expired then a new one is created.
func (l *Instance) Session(ctx context.Context) (sqlliveness.Session, error) {
	if l.testKnobs.SessionOverride != nil {
		if s, err := l.testKnobs.SessionOverride(ctx); s != nil || err != nil {
			return s, err
		}
	}

	for {
		s, ch := l.getSessionOrBlockCh()
		if s != nil {
			return s, nil
		}

		select {
		case <-l.stopper.ShouldQuiesce():
			return nil, stop.ErrUnavailable
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
			var err error
			func() {
				l.mu.Lock()
				defer l.mu.Unlock()
				err = l.mu.startErr
			}()
			if err != nil {
				return nil, err
			}
		}
	}
}

type dummySessionEventListener struct{}

var _ SessionEventListener = &dummySessionEventListener{}

// OnSessionDeleted implements the slinstance.SessionEventListener interface.
func (d *dummySessionEventListener) OnSessionDeleted(context.Context) {}
