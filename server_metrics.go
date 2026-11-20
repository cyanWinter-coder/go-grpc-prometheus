package grpc_prometheus

import (
	"context"
	"github.com/grpc-ecosystem/go-grpc-prometheus/packages/grpcstatus"
	prom "github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc"
)

// ServerMetrics represents a collection of metrics to be registered on a
// Prometheus metrics registry for a gRPC server.
type ServerMetrics struct {
	serverStartedCounter          *prom.CounterVec
	serverHandledCounter          *prom.CounterVec
	serverStreamMsgReceived       *prom.CounterVec
	serverStreamMsgSent           *prom.CounterVec
	serverHandledHistogramEnabled bool
	serverHandledHistogramOpts    prom.HistogramOpts
	serverHandledHistogram        *prom.HistogramVec

	// cardinalityGuard, when set, bounds the number of distinct
	// grpc_service/grpc_method label pairs exported by every family above.
	cardinalityGuard *CardinalityGuard
}

// NewServerMetrics returns a ServerMetrics object. Use a new instance of
// ServerMetrics when not using the default Prometheus metrics registry, for
// example when wanting to control which metrics are added to a registry as
// opposed to automatically adding metrics via init functions.
func NewServerMetrics(counterOpts ...CounterOption) *ServerMetrics {
	opts := counterOptions(counterOpts)
	return &ServerMetrics{
		serverStartedCounter: prom.NewCounterVec(
			opts.apply(prom.CounterOpts{
				Name: "grpc_server_started_total",
				Help: "Total number of RPCs started on the server.",
			}), []string{"grpc_type", "grpc_service", "grpc_method"}),
		serverHandledCounter: prom.NewCounterVec(
			opts.apply(prom.CounterOpts{
				Name: "grpc_server_handled_total",
				Help: "Total number of RPCs completed on the server, regardless of success or failure.",
			}), []string{"grpc_type", "grpc_service", "grpc_method", "grpc_code"}),
		serverStreamMsgReceived: prom.NewCounterVec(
			opts.apply(prom.CounterOpts{
				Name: "grpc_server_msg_received_total",
				Help: "Total number of RPC stream messages received on the server.",
			}), []string{"grpc_type", "grpc_service", "grpc_method"}),
		serverStreamMsgSent: prom.NewCounterVec(
			opts.apply(prom.CounterOpts{
				Name: "grpc_server_msg_sent_total",
				Help: "Total number of gRPC stream messages sent by the server.",
			}), []string{"grpc_type", "grpc_service", "grpc_method"}),
		serverHandledHistogramEnabled: false,
		serverHandledHistogramOpts: prom.HistogramOpts{
			Name:    "grpc_server_handling_seconds",
			Help:    "Histogram of response latency (seconds) of gRPC that had been application-level handled by the server.",
			Buckets: prom.DefBuckets,
		},
		serverHandledHistogram: nil,
	}
}

// EnableHandlingTimeHistogram enables histograms being registered when
// registering the ServerMetrics on a Prometheus registry. Histograms can be
// expensive on Prometheus servers. It takes options to configure histogram
// options such as the defined buckets.
func (m *ServerMetrics) EnableHandlingTimeHistogram(opts ...HistogramOption) {
	for _, o := range opts {
		o(&m.serverHandledHistogramOpts)
	}
	if !m.serverHandledHistogramEnabled {
		m.serverHandledHistogram = prom.NewHistogramVec(
			m.serverHandledHistogramOpts,
			[]string{"grpc_type", "grpc_service", "grpc_method"},
		)
		// Register the late-enabled histogram with the guard as well, so
		// evicted methods cannot leak histogram child series.
		if guard := m.cardinalityGuard; guard != nil {
			histogram := m.serverHandledHistogram
			guard.addFamily(func(rpcType grpcType, serviceName, methodName string) {
				histogram.DeleteLabelValues(string(rpcType), serviceName, methodName)
			})
		}
	}
	m.serverHandledHistogramEnabled = true
}

// EnableCardinalityProtection bounds the number of distinct
// grpc_service/grpc_method label pairs exported by all server-side metric
// families. At most capacity methods are held simultaneously; RPCs for any
// other method are reported under the "unknown" method label so that a
// client inventing dynamic method suffixes can no longer create an
// unbounded number of time series. A slot is reserved when an RPC starts
// and only made permanent when it finishes, so cancelled requests give
// their slot back; idle permanent slots are reclaimed least-recently-used
// together with the underlying Prometheus child metrics, while slots owned
// by in-flight RPCs are never evicted. A non-positive capacity leaves the
// metrics unprotected.
//
// Call it before (or independently of) registering the metrics on a
// Prometheus registry; existing ConstLabels and histogram bucket options
// are unaffected.
func (m *ServerMetrics) EnableCardinalityProtection(capacity int) {
	guard := NewCardinalityGuard(capacity)
	m.cardinalityGuard = guard
	if capacity <= 0 {
		// Disabled guard: Resolve/Commit/Release all pass through.
		return
	}
	guard.addFamily(func(rpcType grpcType, serviceName, methodName string) {
		m.serverStartedCounter.DeleteLabelValues(string(rpcType), serviceName, methodName)
		m.serverStreamMsgReceived.DeleteLabelValues(string(rpcType), serviceName, methodName)
		m.serverStreamMsgSent.DeleteLabelValues(string(rpcType), serviceName, methodName)
		for _, code := range allCodes {
			m.serverHandledCounter.DeleteLabelValues(string(rpcType), serviceName, methodName, code.String())
		}
		if m.serverHandledHistogramEnabled {
			m.serverHandledHistogram.DeleteLabelValues(string(rpcType), serviceName, methodName)
		}
	})
}

// CardinalityGuard returns the guard backing cardinality protection, or nil
// when protection is disabled. Its Stats/Reset/Close methods expose the
// number of tracked labels, merges into unknown and evictions.
func (m *ServerMetrics) CardinalityGuard() *CardinalityGuard {
	return m.cardinalityGuard
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector to the provided channel and returns once
// the last descriptor has been sent.
func (m *ServerMetrics) Describe(ch chan<- *prom.Desc) {
	m.serverStartedCounter.Describe(ch)
	m.serverHandledCounter.Describe(ch)
	m.serverStreamMsgReceived.Describe(ch)
	m.serverStreamMsgSent.Describe(ch)
	if m.serverHandledHistogramEnabled {
		m.serverHandledHistogram.Describe(ch)
	}
}

// Collect is called by the Prometheus registry when collecting
// metrics. The implementation sends each collected metric via the
// provided channel and returns once the last metric has been sent.
func (m *ServerMetrics) Collect(ch chan<- prom.Metric) {
	m.serverStartedCounter.Collect(ch)
	m.serverHandledCounter.Collect(ch)
	m.serverStreamMsgReceived.Collect(ch)
	m.serverStreamMsgSent.Collect(ch)
	if m.serverHandledHistogramEnabled {
		m.serverHandledHistogram.Collect(ch)
	}
}

// UnaryServerInterceptor is a gRPC server-side interceptor that provides Prometheus monitoring for Unary RPCs.
func (m *ServerMetrics) UnaryServerInterceptor() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		monitor := newServerReporter(m, Unary, info.FullMethod)
		monitor.ReceivedMessage()
		resp, err := handler(ctx, req)
		st, _ := grpcstatus.FromError(err)
		monitor.Handled(st.Code())
		if err == nil {
			monitor.SentMessage()
		}
		return resp, err
	}
}

// StreamServerInterceptor is a gRPC server-side interceptor that provides Prometheus monitoring for Streaming RPCs.
func (m *ServerMetrics) StreamServerInterceptor() func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		monitor := newServerReporter(m, streamRPCType(info), info.FullMethod)
		err := handler(srv, &monitoredServerStream{ss, monitor})
		st, _ := grpcstatus.FromError(err)
		monitor.Handled(st.Code())
		return err
	}
}

// InitializeMetrics initializes all metrics, with their appropriate null
// value, for all gRPC methods registered on a gRPC server. This is useful, to
// ensure that all metrics exist when collecting and querying.
func (m *ServerMetrics) InitializeMetrics(server *grpc.Server) {
	serviceInfo := server.GetServiceInfo()
	for serviceName, info := range serviceInfo {
		for _, mInfo := range info.Methods {
			preRegisterMethod(m, serviceName, &mInfo)
		}
	}
}

func streamRPCType(info *grpc.StreamServerInfo) grpcType {
	if info.IsClientStream && !info.IsServerStream {
		return ClientStream
	} else if !info.IsClientStream && info.IsServerStream {
		return ServerStream
	}
	return BidiStream
}

// monitoredStream wraps grpc.ServerStream allowing each Sent/Recv of message to increment counters.
type monitoredServerStream struct {
	grpc.ServerStream
	monitor *serverReporter
}

func (s *monitoredServerStream) SendMsg(m interface{}) error {
	err := s.ServerStream.SendMsg(m)
	if err == nil {
		s.monitor.SentMessage()
	}
	return err
}

func (s *monitoredServerStream) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		s.monitor.ReceivedMessage()
	}
	return err
}

// preRegisterMethod is invoked on Register of a Server, allowing all gRPC services labels to be pre-populated.
func preRegisterMethod(metrics *ServerMetrics, serviceName string, mInfo *grpc.MethodInfo) {
	methodName := mInfo.Name
	methodType := string(typeFromMethodInfo(mInfo))
	guard := metrics.cardinalityGuard
	if guard != nil {
		// Registered service methods are known and finite: give them a
		// permanent slot up front without evicting other methods. When the
		// table is full, pre-register under unknown instead of creating new
		// series. Pre-registration is not an RPC and does not count as a
		// merge.
		if !guard.TryCommit(typeFromMethodInfo(mInfo), serviceName, methodName) {
			methodName = UnknownMethodLabel
		}
	}
	// These are just references (no increments), as just referencing will create the labels but not set values.
	metrics.serverStartedCounter.GetMetricWithLabelValues(methodType, serviceName, methodName)
	metrics.serverStreamMsgReceived.GetMetricWithLabelValues(methodType, serviceName, methodName)
	metrics.serverStreamMsgSent.GetMetricWithLabelValues(methodType, serviceName, methodName)
	if metrics.serverHandledHistogramEnabled {
		metrics.serverHandledHistogram.GetMetricWithLabelValues(methodType, serviceName, methodName)
	}
	for _, code := range allCodes {
		metrics.serverHandledCounter.GetMetricWithLabelValues(methodType, serviceName, methodName, code.String())
	}
}
