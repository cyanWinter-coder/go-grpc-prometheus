// Copyright 2016 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_prometheus

import (
	"container/list"
	"sync"
)

// UnknownMethodLabel is the grpc_method label value assigned to RPCs whose
// method is not tracked by the cardinality guard, either because the guard
// is full or has been closed. It keeps the number of exported label
// combinations bounded.
const UnknownMethodLabel = "unknown"

// CardinalityStats carries the internal counters of a CardinalityGuard.
type CardinalityStats struct {
	// Tracked is the number of method labels currently held in the
	// allocation table, including tentative slots of in-flight RPCs.
	Tracked int
	// Merged is the number of RPCs that were reported under the unknown
	// method label because their method did not hold a slot.
	Merged int
	// Evicted is the number of permanent method labels that lost their slot
	// to make room for a different method.
	Evicted int
}

// CardinalityGuard limits the cardinality of the grpc_service/grpc_method
// label pairs produced by the monitoring interceptors. At most capacity
// distinct methods are held at any time; methods without a slot are
// reported under UnknownMethodLabel so that the set of exported Prometheus
// time series stays bounded even when RPCs carry arbitrary, dynamic method
// suffixes.
//
// Allocation is two-phase. Each RPC reserves a slot through Resolve: a
// reservation for a brand-new method creates a tentative entry that is not
// yet permanent, and cancelling the RPC (Release) gives that entry back, so
// cancelled requests never consume permanent capacity. On normal
// completion Commit marks the entry permanent. When the table is full,
// Resolve first reclaims the least-recently-used permanent slot that has no
// in-flight RPC (deleting the method's child metrics from every guarded
// family); if every slot is busy, the new method folds into unknown instead
// of evicting an in-flight RPC.
type CardinalityGuard struct {
	mu       sync.Mutex
	capacity int
	// entries holds the methods that currently occupy a slot.
	entries map[string]*cardinalityEntry
	// reservations counts in-flight RPCs per method key, including RPCs
	// that were folded into unknown.
	reservations map[string]int
	lru          *list.List
	families     []cardinalityFamily
	closed       bool
	merged       int
	evicted      int
}

type cardinalityEntry struct {
	// element is the LRU list element, whose Value is the entry key.
	element *list.Element
	// committed distinguishes permanent slots (at least one finished RPC)
	// from tentative slots owned solely by in-flight RPCs.
	committed bool
	// rpcType, service and method allow eviction to delete the child
	// metrics of every guarded family without consulting the reporter.
	rpcType grpcType
	service string
	method  string
}

// cardinalityFamily binds a deletion callback to a metric family. The
// callback removes every child metric (one per grpc_code for handled
// counters, a single one otherwise) belonging to an evicted method.
type cardinalityFamily struct {
	delete func(rpcType grpcType, service, method string)
}

// NewCardinalityGuard creates a guard holding at most capacity distinct
// method labels. Capacity must be positive; with a non-positive value the
// returned guard is disabled and Resolve always returns the real method
// label, preserving the unprotected behaviour.
func NewCardinalityGuard(capacity int) *CardinalityGuard {
	g := &CardinalityGuard{
		capacity:     capacity,
		entries:      map[string]*cardinalityEntry{},
		reservations: map[string]int{},
		lru:          list.New(),
	}
	if capacity <= 0 {
		// Keep the guard inert: every lookup passes through unchanged.
		g.capacity = 0
	}
	return g
}

// addFamily registers a metric family whose child metrics must be removed
// when a method label is evicted. It is called while wiring the guard into
// ServerMetrics/ClientMetrics and before the guard serves any RPC.
func (g *CardinalityGuard) addFamily(delete func(rpcType grpcType, service, method string)) {
	g.families = append(g.families, cardinalityFamily{delete: delete})
}

func cardinalityKey(rpcType grpcType, service, method string) string {
	return string(rpcType) + "\x00" + service + "\x00" + method
}

// Resolve maps an RPC to the method label it must be reported under and
// records an in-flight reservation for that method. A method that already
// holds a slot returns its real label and is refreshed in the LRU order. A
// brand-new method is admitted tentatively while permanent capacity
// remains; otherwise it is folded into UnknownMethodLabel and the merge is
// counted. Every successful Resolve must be paired with either Commit (RPC
// finished normally) or Release (RPC cancelled).
func (g *CardinalityGuard) Resolve(rpcType grpcType, service, method string) string {
	if g == nil {
		return method
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.capacity == 0 {
		return method
	}
	key := cardinalityKey(rpcType, service, method)
	g.reservations[key]++
	if entry, ok := g.entries[key]; ok {
		g.lru.MoveToFront(entry.element)
		return method
	}
	// Brand-new method. Reclaim an idle permanent slot first so that cold
	// methods do not permanently block the whole capacity; tentative and
	// in-flight slots are never evicted.
	if !g.closed && g.lru.Len() >= g.capacity {
		if !g.evictLocked() {
			g.merged++
			return UnknownMethodLabel
		}
	}
	if !g.closed {
		// Tentative admission: the slot becomes permanent only once an RPC
		// for the method finishes. A cancellation releases it via Release.
		g.entries[key] = &cardinalityEntry{
			element: g.lru.PushFront(key),
			rpcType: rpcType,
			service: service,
			method:  method,
		}
		return method
	}
	g.merged++
	return UnknownMethodLabel
}

// Release abandons the reservation made by Resolve for a cancelled RPC.
// When the method only held a tentative slot (no RPC has committed it yet
// and no other RPC is still in flight for it), the slot is handed back
// without counting an eviction. Cancelled RPCs therefore never occupy
// permanent capacity.
func (g *CardinalityGuard) Release(rpcType grpcType, service, method string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.capacity == 0 {
		return
	}
	key := cardinalityKey(rpcType, service, method)
	// Always balance the Resolve reservation, including RPCs that folded
	// into unknown; otherwise the reservations map would grow without bound.
	g.dropReservationLocked(key)
	if method == UnknownMethodLabel {
		return
	}
	entry, ok := g.entries[key]
	if !ok || entry.committed || g.reservations[key] > 0 {
		return
	}
	delete(g.entries, key)
	g.lru.Remove(entry.element)
}

// Commit marks the RPC's reservation as finished. A tentative slot becomes
// permanent; a reservation that folded into unknown stays folded and only
// drops its bookkeeping, so it never creates a slot retroactively.
// Concurrent commits of the same method produce exactly one permanent
// entry.
func (g *CardinalityGuard) Commit(rpcType grpcType, service, method string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.capacity == 0 {
		return
	}
	key := cardinalityKey(rpcType, service, method)
	// Always balance the Resolve reservation, including RPCs that folded
	// into unknown; otherwise the reservations map would grow without bound.
	g.dropReservationLocked(key)
	if method == UnknownMethodLabel {
		return
	}
	if entry, ok := g.entries[key]; ok {
		entry.committed = true
		g.lru.MoveToFront(entry.element)
	}
}

// TryCommit allocates a permanent slot without evicting existing labels: it
// returns false when the table is full (or closed), leaving the caller to
// fold the method into unknown. It is meant for pre-registering the finite
// set of methods declared on a gRPC server.
func (g *CardinalityGuard) TryCommit(rpcType grpcType, service, method string) bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.capacity == 0 || method == UnknownMethodLabel {
		return g.capacity == 0
	}
	if g.closed {
		_, ok := g.entries[cardinalityKey(rpcType, service, method)]
		return ok
	}
	key := cardinalityKey(rpcType, service, method)
	if entry, ok := g.entries[key]; ok {
		entry.committed = true
		g.lru.MoveToFront(entry.element)
		return true
	}
	if g.lru.Len() >= g.capacity {
		return false
	}
	g.entries[key] = &cardinalityEntry{
		element:   g.lru.PushFront(key),
		committed: true,
		rpcType:   rpcType,
		service:   service,
		method:    method,
	}
	return true
}

func (g *CardinalityGuard) dropReservationLocked(key string) {
	n := g.reservations[key]
	if n <= 1 {
		delete(g.reservations, key)
	} else {
		g.reservations[key] = n - 1
	}
}

// evictLocked reclaims the least-recently-used permanent slot that has no
// in-flight RPC and deletes the method's child metrics from every guarded
// family. It is a no-op when every slot is tentative or busy, in which case
// the caller must fold the new method into unknown. Callers must hold g.mu;
// the family callbacks use their own metric locks, never g.mu.
func (g *CardinalityGuard) evictLocked() bool {
	for candidate := g.lru.Back(); candidate != nil; candidate = candidate.Prev() {
		key := candidate.Value.(string)
		entry := g.entries[key]
		if !entry.committed || g.reservations[key] > 0 {
			continue
		}
		delete(g.entries, key)
		g.lru.Remove(candidate)
		g.evicted++
		for _, family := range g.families {
			family.delete(entry.rpcType, entry.service, entry.method)
		}
		return true
	}
	return false
}

// Stats reports how many labels are tracked, how many RPCs were merged into
// the unknown label and how many permanent labels were evicted.
func (g *CardinalityGuard) Stats() CardinalityStats {
	if g == nil {
		return CardinalityStats{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return CardinalityStats{
		Tracked: len(g.entries),
		Merged:  g.merged,
		Evicted: g.evicted,
	}
}

// Reset clears the allocation table, reservations and the merge/eviction
// counters without deleting any metric already registered with Prometheus.
// Afterwards every method starts untracked again and existing time series
// keep their values.
func (g *CardinalityGuard) Reset() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.entries = map[string]*cardinalityEntry{}
	g.reservations = map[string]int{}
	g.lru.Init()
	g.merged = 0
	g.evicted = 0
}

// Close stops admitting new methods. Entries already in the table keep
// their slots (and LRU order); every other method is merged into unknown
// from now on.
func (g *CardinalityGuard) Close() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
}

// Closed reports whether Close has been called.
func (g *CardinalityGuard) Closed() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}
