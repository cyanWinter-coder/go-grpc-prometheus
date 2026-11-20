// Copyright 2016 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_prometheus

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	pb_testproto "github.com/grpc-ecosystem/go-grpc-prometheus/examples/testproto"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func newProtectedServerMetrics(capacity int) *ServerMetrics {
	m := NewServerMetrics(WithConstLabels(prometheus.Labels{"app": "test"}))
	m.EnableHandlingTimeHistogram(WithHistogramBuckets([]float64{0.001, 0.01}))
	m.EnableCardinalityProtection(capacity)
	return m
}

// Reservations made for merged (unknown) or cancelled RPCs are always
// released, so the internal reservation bookkeeping cannot leak.
func TestReservationsDoNotLeak(t *testing.T) {
	m := newProtectedServerMetrics(1)
	guard := m.CardinalityGuard()

	// Occupy the only slot with an in-flight RPC so all traffic merges.
	busy := newServerReporter(m, Unary, "/tenant.Svc/Busy")
	for i := 0; i < 100; i++ {
		r := newServerReporter(m, Unary, "/tenant.Svc/Flood"+strconv.Itoa(i))
		if i%3 == 0 {
			r.Handled(codes.Canceled)
		} else {
			r.Handled(codes.OK)
		}
	}
	busy.Handled(codes.OK)

	guard.mu.Lock()
	assert.Empty(t, guard.reservations, "every Resolve must be balanced by Commit/Release")
	guard.mu.Unlock()

	// Same guarantee after Reset.
	guard.Reset()
	guard.mu.Lock()
	assert.Empty(t, guard.reservations)
	guard.mu.Unlock()
}

// Late-enabled histograms (protection turned on first) must still be kept
// bounded: their child metrics are deleted on eviction.
func TestLateEnabledHistogramIsGuarded(t *testing.T) {
	metrics := NewServerMetrics()
	metrics.EnableCardinalityProtection(1)
	metrics.EnableHandlingTimeHistogram() // enabled after protection

	r := newServerReporter(metrics, Unary, "/tenant.Svc/One")
	r.Handled(codes.OK)
	r = newServerReporter(metrics, Unary, "/tenant.Svc/Two")
	r.Handled(codes.OK)

	assert.Equal(t, 1, metrics.CardinalityGuard().Stats().Evicted)
	assert.False(t, metrics.serverHandledHistogram.DeleteLabelValues("unary", "tenant.Svc", "One"),
		"late-enabled histogram child must be evicted too")
}

// A disabled guard must be completely transparent: real method labels pass
// through, no entries are created and Stats stay zeroed.
func TestCardinalityGuardDisabledIsTransparent(t *testing.T) {
	m := NewServerMetrics()
	m.EnableCardinalityProtection(0)
	guard := m.CardinalityGuard()
	require.NotNil(t, guard)

	for i := 0; i < 1000; i++ {
		r := newServerReporter(m, Unary, "/svc/Method"+strconv.Itoa(i))
		r.Handled(codes.OK)
	}
	assert.Equal(t, CardinalityStats{}, guard.Stats())
}

// Fixed capacity: while all slots are busy with in-flight RPCs, new
// methods fold into unknown; every metric family (including the histogram
// and status codes) still receives the totals. Once slots free up, normal
// LRU admission resumes.
func TestServerCardinalityFoldsOverflowIntoUnknown(t *testing.T) {
	m := newProtectedServerMetrics(2)
	guard := m.CardinalityGuard()

	// Two long-lived RPCs occupy every slot with tentative entries.
	busyOne := newServerReporter(m, Unary, "/tenant.Svc/One")
	busyTwo := newServerReporter(m, Unary, "/tenant.Svc/Two")
	require.Equal(t, 2, guard.Stats().Tracked)

	// Busy slots cannot be evicted, so overflow traffic merges.
	for _, method := range []string{"Three", "Four"} {
		r := newServerReporter(m, Unary, "/tenant.Svc/"+method)
		r.ReceivedMessage()
		r.Handled(codes.OK)
	}
	stats := guard.Stats()
	assert.Equal(t, 2, stats.Tracked, "capacity caps tracked labels")
	assert.Equal(t, 2, stats.Merged, "overflow RPCs fold into unknown")
	assert.Equal(t, 0, stats.Evicted, "in-flight slots are never evicted")

	// The two overflow RPCs land in unknown, one started + handled each.
	requireValue(t, 2, m.serverStartedCounter.WithLabelValues("unary", "tenant.Svc", UnknownMethodLabel))
	requireValue(t, 2, m.serverHandledCounter.WithLabelValues("unary", "tenant.Svc", UnknownMethodLabel, "OK"))
	requireValue(t, 2, m.serverStreamMsgReceived.WithLabelValues("unary", "tenant.Svc", UnknownMethodLabel))
	requireValueHistCount(t, 2, m.serverHandledHistogram.WithLabelValues("unary", "tenant.Svc", UnknownMethodLabel))

	// A failed overflow RPC is recorded under unknown with its status code.
	r := newServerReporter(m, Unary, "/tenant.Svc/Three")
	r.Handled(codes.Internal)
	requireValue(t, 1, m.serverHandledCounter.WithLabelValues("unary", "tenant.Svc", UnknownMethodLabel, "Internal"))
	assert.Equal(t, 3, guard.Stats().Merged)

	// Once the busy RPCs finish, their slots become permanent and free for
	// LRU reuse: a new method is admitted instead of merged.
	busyOne.Handled(codes.OK)
	busyTwo.Handled(codes.OK)
	before := guard.Stats().Merged
	r = newServerReporter(m, Unary, "/tenant.Svc/Five")
	r.Handled(codes.OK)
	assert.Equal(t, before, guard.Stats().Merged, "freed capacity admits the new method")
	assert.Equal(t, 2, guard.Stats().Tracked)
}

// LRU eviction must remove the slot AND the underlying Prometheus child
// metrics of the least-recently-used idle method, incrementing eviction
// stats, while later traffic for the evicted method is merged into unknown.
func TestServerCardinalityEvictionDeletesChildMetrics(t *testing.T) {
	m := newProtectedServerMetrics(2)
	guard := m.CardinalityGuard()

	commit := func(method string, code codes.Code) {
		r := newServerReporter(m, Unary, "/tenant.Svc/"+method)
		r.Handled(code)
	}
	commit("One", codes.OK)
	commit("Two", codes.OK)
	// Touch Two so One (untouched since) becomes the LRU victim when Three
	// needs the slot.
	commit("Two", codes.OK)
	commit("Three", codes.OK)

	stats := guard.Stats()
	assert.Equal(t, 2, stats.Tracked)
	assert.Equal(t, 1, stats.Evicted)

	// One's children are gone from every family.
	assert.False(t, m.serverHandledCounter.DeleteLabelValues("unary", "tenant.Svc", "One", "OK"),
		"evicted handled counter must already be gone")
	assert.False(t, m.serverStartedCounter.DeleteLabelValues("unary", "tenant.Svc", "One"),
		"evicted started counter must already be gone")
	assert.False(t, m.serverHandledHistogram.DeleteLabelValues("unary", "tenant.Svc", "One"),
		"evicted histogram must already be gone")

	// Subsequent traffic for the evicted method gets admitted again (Two is
	// now the LRU idle label and gets evicted instead), keeping the table at
	// capacity and driving the eviction counter.
	commit("One", codes.OK)
	assert.Equal(t, 2, guard.Stats().Tracked)
	assert.Equal(t, 2, guard.Stats().Evicted)
	assert.False(t, m.serverStartedCounter.DeleteLabelValues("unary", "tenant.Svc", "Two"))
}

// In-flight (tentative) slots are protected from eviction: a burst of
// concurrent brand-new methods never exceeds capacity and never evicts a
// method whose RPC is still running.
func TestInFlightSlotsAreNotEvicted(t *testing.T) {
	m := newProtectedServerMetrics(2)
	guard := m.CardinalityGuard()

	// Two RPCs start but do not finish: both hold tentative slots.
	first := newServerReporter(m, Unary, "/tenant.Svc/First")
	second := newServerReporter(m, Unary, "/tenant.Svc/Second")
	assert.Equal(t, 2, guard.Stats().Tracked)

	// A third RPC cannot evict either busy slot, so it folds into unknown.
	third := newServerReporter(m, Unary, "/tenant.Svc/Third")
	assert.Equal(t, 2, guard.Stats().Tracked)
	assert.Equal(t, 1, guard.Stats().Merged)
	requireValue(t, 1, m.serverStartedCounter.WithLabelValues("unary", "tenant.Svc", UnknownMethodLabel))

	// Finishing the first two makes their slots permanent; the third's
	// completion stays folded and never retro-creates a series.
	first.Handled(codes.OK)
	second.Handled(codes.OK)
	third.Handled(codes.OK)
	assert.Equal(t, 2, guard.Stats().Tracked)
	assert.True(t, m.serverStartedCounter.DeleteLabelValues("unary", "tenant.Svc", "Third") == false,
		"the folded RPC must never create its own series")
}

// Reset only clears protection state; already registered metrics keep their
// values, and the next RPCs start from an empty allocation table.
func TestCardinalityGuardResetKeepsRegisteredMetrics(t *testing.T) {
	m := newProtectedServerMetrics(4)
	guard := m.CardinalityGuard()

	r := newServerReporter(m, Unary, "/tenant.Svc/One")
	r.Handled(codes.OK)
	require.Equal(t, 1, guard.Stats().Tracked)
	requireValue(t, 1, m.serverHandledCounter.WithLabelValues("unary", "tenant.Svc", "One", "OK"))

	guard.Reset()
	assert.Equal(t, CardinalityStats{}, guard.Stats())

	// The counter itself is untouched: reset only clears the guard, it does
	// not delete registered Prometheus metrics.
	requireValue(t, 1, m.serverHandledCounter.WithLabelValues("unary", "tenant.Svc", "One", "OK"))

	// First new RPC after reset is untracked again (merged), then commits.
	r = newServerReporter(m, Unary, "/tenant.Svc/Two")
	r.Handled(codes.OK)
	assert.Equal(t, 1, guard.Stats().Tracked)
	assert.Equal(t, 0, guard.Stats().Merged, "free capacity means no merge after reset")
}

// After Close no new methods are admitted; existing slots keep working and
// are never evicted.
func TestCardinalityGuardCloseStopsAllocation(t *testing.T) {
	m := newProtectedServerMetrics(2)
	guard := m.CardinalityGuard()

	r := newServerReporter(m, Unary, "/tenant.Svc/One")
	r.Handled(codes.OK)
	assert.False(t, guard.Closed())
	guard.Close()
	assert.True(t, guard.Closed())

	for _, method := range []string{"Two", "Three"} {
		r := newServerReporter(m, Unary, "/tenant.Svc/"+method)
		r.Handled(codes.OK)
	}
	assert.Equal(t, 1, guard.Stats().Tracked, "no new slots after close")
	assert.Equal(t, 2, guard.Stats().Merged, "new methods fold into unknown")
	assert.Equal(t, 0, guard.Stats().Evicted, "closing must not evict tracked labels")

	// The already tracked method still resolves to its real label.
	r = newServerReporter(m, Unary, "/tenant.Svc/One")
	r.Handled(codes.OK)
	requireValue(t, 2, m.serverStartedCounter.WithLabelValues("unary", "tenant.Svc", "One"))
}

// Cancelled requests must never consume a permanent slot, and give back a
// tentative slot so the capacity stays available.
func TestCancelledRequestDoesNotConsumeSlot(t *testing.T) {
	m := newProtectedServerMetrics(1)
	guard := m.CardinalityGuard()

	r := newServerReporter(m, Unary, "/tenant.Svc/FlashInThePan")
	assert.Equal(t, 1, guard.Stats().Tracked, "the in-flight RPC holds a tentative slot")
	r.Handled(codes.Canceled)
	assert.Equal(t, 0, guard.Stats().Tracked, "cancellation hands the slot back")

	// The capacity is still fully available for a real method.
	r = newServerReporter(m, Unary, "/tenant.Svc/Real")
	r.Handled(codes.OK)
	assert.Equal(t, 1, guard.Stats().Tracked)
	assert.Equal(t, 0, guard.Stats().Evicted)

	// The cancelled RPC's totals are still recorded under its own label for
	// the duration of the request; only the slot is handed back afterwards.
	requireValue(t, 1, m.serverHandledCounter.WithLabelValues("unary", "tenant.Svc", "FlashInThePan", "Canceled"))
}

// Concurrent RPCs for the same brand-new method create exactly one entry;
// mixed-in cancellations never leave dangling slots. Run with -race for the
// locking claim.
func TestConcurrentAllocationIsSingleEntry(t *testing.T) {
	m := newProtectedServerMetrics(16)
	guard := m.CardinalityGuard()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := newServerReporter(m, Unary, "/tenant.Svc/Hot")
			if i%5 == 0 {
				r.Handled(codes.Canceled)
				return
			}
			r.Handled(codes.OK)
		}(i)
	}
	wg.Wait()

	stats := guard.Stats()
	assert.Equal(t, 1, stats.Tracked, "exactly one entry for one method")
	assert.Equal(t, 0, stats.Evicted)
	assert.Equal(t, 0, stats.Merged, "capacity never exhausted by a single method")
	requireValue(t, 200, m.serverStartedCounter.WithLabelValues("unary", "tenant.Svc", "Hot"))
	requireValue(t, 40, m.serverHandledCounter.WithLabelValues("unary", "tenant.Svc", "Hot", "Canceled"))
}

// A high-cardinality flood cannot grow the exported time series beyond the
// configured capacity. When the whole capacity is held by in-flight RPCs,
// every additional concurrent RPC folds into unknown, and its totals still
// land on the unknown series.
func TestHighCardinalityFloodStaysBounded(t *testing.T) {
	capacity := 5
	m := newProtectedServerMetrics(capacity)
	guard := m.CardinalityGuard()

	// Fill the table with still-running RPCs: their tentative slots cannot
	// be evicted.
	busy := make([]*serverReporter, capacity)
	for i := range busy {
		busy[i] = newServerReporter(m, ServerStream, "/tenant.Svc/Busy"+strconv.Itoa(i))
	}
	require.Equal(t, capacity, guard.Stats().Tracked)

	const overflow = 50
	var wg sync.WaitGroup
	for i := 0; i < overflow; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := newServerReporter(m, ServerStream, "/tenant.Svc/Flood"+strconv.Itoa(i))
			r.ReceivedMessage()
			r.SentMessage()
			r.Handled(codes.OK)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, capacity, guard.Stats().Tracked, "flood never grows the table beyond capacity")
	assert.Equal(t, overflow, guard.Stats().Merged)
	assert.Equal(t, 0, guard.Stats().Evicted, "in-flight slots cannot be reclaimed")

	// Gather through a real registry: the number of method label sets on
	// every family is bounded (tracked methods + unknown).
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(m))
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		methodValues := map[string]struct{}{}
		for _, metric := range fam.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "grpc_method" {
					methodValues[label.GetValue()] = struct{}{}
				}
			}
		}
		assert.True(t, len(methodValues) <= capacity+1,
			"family %s exports %d method labels, more than capacity allows", fam.GetName(), len(methodValues))
	}

	// Totals are preserved on the unknown bucket regardless of the flood.
	requireValue(t, overflow, m.serverStartedCounter.WithLabelValues("server_stream", "tenant.Svc", UnknownMethodLabel))
	requireValue(t, overflow, m.serverStreamMsgReceived.WithLabelValues("server_stream", "tenant.Svc", UnknownMethodLabel))
	requireValue(t, overflow, m.serverStreamMsgSent.WithLabelValues("server_stream", "tenant.Svc", UnknownMethodLabel))
	requireValueHistCount(t, overflow, m.serverHandledHistogram.WithLabelValues("server_stream", "tenant.Svc", UnknownMethodLabel))

	// Releasing the busy RPCs leaves exactly the permanent slots behind.
	for _, r := range busy {
		r.Handled(codes.OK)
	}
	assert.Equal(t, capacity, guard.Stats().Tracked)
}

// A sequential churn of unique methods (the classic dynamic-suffix traffic
// pattern) stays bounded through LRU eviction of idle labels: eviction
// removes the Prometheus child metrics, so exported series never accumulate.
func TestSequentialMethodChurnStaysBounded(t *testing.T) {
	capacity := 5
	m := newProtectedServerMetrics(capacity)
	guard := m.CardinalityGuard()

	const churn = 500
	for i := 0; i < churn; i++ {
		r := newServerReporter(m, Unary, "/tenant.Svc/Method"+strconv.Itoa(i))
		r.Handled(codes.OK)
	}
	assert.Equal(t, capacity, guard.Stats().Tracked)
	assert.Equal(t, 0, guard.Stats().Merged, "idle slots are always evictable, so nothing merges")
	assert.Equal(t, churn-capacity, guard.Stats().Evicted)

	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(m))
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		methodValues := map[string]struct{}{}
		for _, metric := range fam.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "grpc_method" {
					methodValues[label.GetValue()] = struct{}{}
				}
			}
		}
		assert.True(t, len(methodValues) <= capacity,
			"family %s exports %d method labels, more than capacity allows", fam.GetName(), len(methodValues))
	}
}

// End-to-end through real gRPC interceptors, including streaming messages
// and error codes.
func TestProtectedInterceptorsEndToEnd(t *testing.T) {
	metrics := NewServerMetrics()
	metrics.EnableHandlingTimeHistogram()
	metrics.EnableCardinalityProtection(1)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(metrics.UnaryServerInterceptor()),
		grpc.StreamInterceptor(metrics.StreamServerInterceptor()),
	)
	pb_testproto.RegisterTestServiceServer(server, &testService{t: t})
	go server.Serve(listener)
	defer server.Stop()

	conn, err := grpc.Dial(listener.Addr().String(), grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(2*time.Second))
	require.NoError(t, err)
	defer conn.Close()
	client := pb_testproto.NewTestServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	service := "mwitkow.testproto.TestService"

	// Phase 1: sequential traffic is admitted normally via LRU eviction;
	// success, failure and stream message/histogram metrics are all recorded
	// on real method labels while the table stays at capacity one.
	_, err = client.PingEmpty(ctx, &pb_testproto.Empty{})
	require.NoError(t, err)
	_, err = client.PingError(ctx, &pb_testproto.PingRequest{ErrorCodeReturned: uint32(codes.FailedPrecondition)})
	require.Error(t, err)
	stream, err := client.PingList(ctx, &pb_testproto.PingRequest{})
	require.NoError(t, err)
	for {
		_, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		require.NoError(t, recvErr)
	}
	guard := metrics.CardinalityGuard()
	assert.Equal(t, 1, guard.Stats().Tracked)
	assert.Equal(t, 0, guard.Stats().Merged)
	assert.Equal(t, 2, guard.Stats().Evicted)
	requireValue(t, countListResponses, metrics.serverStreamMsgSent.WithLabelValues("server_stream", service, "PingList"))
	requireValueHistCount(t, 1, metrics.serverHandledHistogram.WithLabelValues("server_stream", service, "PingList"))

	// Phase 2: hold the only slot with an in-flight RPC so subsequent real
	// traffic folds into unknown, including its status code and totals.
	busy := newServerReporter(metrics, Unary, "/"+service+"/PingEmpty")
	_, err = client.PingError(ctx, &pb_testproto.PingRequest{ErrorCodeReturned: uint32(codes.FailedPrecondition)})
	require.Error(t, err)
	assert.Equal(t, 1, guard.Stats().Merged)
	requireValue(t, 1, metrics.serverHandledCounter.WithLabelValues("unary", service, UnknownMethodLabel, "FailedPrecondition"))
	busy.Handled(codes.OK)
}

// Client-side protection mirrors the server guard, including the client
// stream send/receive histograms.
func TestClientCardinalityProtection(t *testing.T) {
	metrics := NewClientMetrics()
	metrics.EnableClientHandlingTimeHistogram()
	metrics.EnableClientStreamReceiveTimeHistogram()
	metrics.EnableClientStreamSendTimeHistogram()
	metrics.EnableCardinalityProtection(1)
	guard := metrics.CardinalityGuard()

	// A cancelled client RPC gives back its tentative slot; run it first so
	// no permanent labels exist yet.
	cancelled := newClientReporter(metrics, Unary, "/tenant.Svc/Cancelled")
	assert.Equal(t, 1, guard.Stats().Tracked)
	cancelled.Handled(codes.Canceled)
	assert.Equal(t, 0, guard.Stats().Tracked, "cancellation hands the tentative slot back")
	requireValue(t, 1, metrics.clientHandledCounter.WithLabelValues("unary", "tenant.Svc", "Cancelled", "Canceled"))

	// One method occupies the only permanent slot.
	r := newClientReporter(metrics, Unary, "/tenant.Svc/One")
	r.SentMessage()
	r.Handled(codes.OK)
	assert.Equal(t, 1, guard.Stats().Tracked)

	// Two further methods overflow sequentially: each evicts the idle
	// predecessor's slot (LRU) and keeps the table at capacity one. Nothing
	// merges, because no RPC is ever in flight when the next one starts.
	for _, method := range []string{"Two", "Three"} {
		r := newClientReporter(metrics, Unary, "/tenant.Svc/"+method)
		r.Handled(codes.Unavailable)
	}
	assert.Equal(t, 1, guard.Stats().Tracked)
	assert.Equal(t, 0, guard.Stats().Merged)
	assert.Equal(t, 2, guard.Stats().Evicted)
	// Two was evicted when Three took its slot, so its short-lived child
	// metrics are already gone; Three's failure total still exists.
	assert.False(t, metrics.clientHandledCounter.DeleteLabelValues("unary", "tenant.Svc", "Two", "Unavailable"))
	requireValue(t, 1, metrics.clientHandledCounter.WithLabelValues("unary", "tenant.Svc", "Three", "Unavailable"))

	// Eviction removes every client family, including both stream histograms.
	assert.False(t, metrics.clientHandledHistogram.DeleteLabelValues("unary", "tenant.Svc", "One"))
	assert.False(t, metrics.clientStreamRecvHistogram.DeleteLabelValues("unary", "tenant.Svc", "One"))
	assert.False(t, metrics.clientStreamSendHistogram.DeleteLabelValues("unary", "tenant.Svc", "One"))

	// With the only slot held in-flight, an additional client RPC merges.
	busy := newClientReporter(metrics, Unary, "/tenant.Svc/Busy")
	merged := newClientReporter(metrics, Unary, "/tenant.Svc/Merged")
	merged.Handled(codes.Unavailable)
	assert.Equal(t, 1, guard.Stats().Merged)
	requireValue(t, 1, metrics.clientHandledCounter.WithLabelValues("unary", "tenant.Svc", UnknownMethodLabel, "Unavailable"))
	busy.Handled(codes.OK)
}

// InitializeMetrics with protection enabled pre-registers known methods,
// folding excess ones into unknown instead of creating series.
func TestInitializeMetricsRespectsProtection(t *testing.T) {
	metrics := NewServerMetrics()
	metrics.EnableHandlingTimeHistogram()
	metrics.EnableCardinalityProtection(1)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	pb_testproto.RegisterTestServiceServer(server, &testService{t: t})
	metrics.InitializeMetrics(server)
	server.Stop()
	listener.Close()

	stats := metrics.CardinalityGuard().Stats()
	assert.Equal(t, 1, stats.Tracked, "only one declared method holds a slot")
	assert.Equal(t, 0, stats.Merged, "pre-registration is not RPC traffic and never counts as a merge")

	// Exactly one declared method kept its own series; the other three were
	// pre-registered under unknown, so every family exports at most those
	// two grpc_method values.
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(metrics))
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		methodValues := map[string]struct{}{}
		for _, metric := range fam.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "grpc_method" {
					methodValues[label.GetValue()] = struct{}{}
				}
			}
		}
		assert.True(t, len(methodValues) <= 2, "family %s leaks method label sets: %v", fam.GetName(), methodValues)
	}
}

// gatherMethodValues collects a metric family and returns the distinct set
// of grpc_method label values it currently exports.
func gatherMethodValues(t *testing.T, collector prometheus.Collector) []string {
	t.Helper()
	ch := make(chan prometheus.Metric, 256)
	go func() {
		collector.Collect(ch)
		close(ch)
	}()
	seen := map[string]struct{}{}
	for metric := range ch {
		pb := &dto.Metric{}
		require.NoError(t, metric.Write(pb))
		for _, label := range pb.GetLabel() {
			if label.GetName() == "grpc_method" {
				seen[label.GetValue()] = struct{}{}
			}
		}
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	return values
}
