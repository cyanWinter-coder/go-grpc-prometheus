// Copyright 2016 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_prometheus

import (
	"time"

	"google.golang.org/grpc/codes"
)

type serverReporter struct {
	metrics     *ServerMetrics
	rpcType     grpcType
	serviceName string
	methodName  string
	// reportedMethod is the grpc_method label value actually used on every
	// metric family; it equals methodName unless cardinality protection
	// folded the RPC into unknown.
	reportedMethod string
	startTime      time.Time
}

func newServerReporter(m *ServerMetrics, rpcType grpcType, fullMethod string) *serverReporter {
	r := &serverReporter{
		metrics: m,
		rpcType: rpcType,
	}
	if r.metrics.serverHandledHistogramEnabled {
		r.startTime = time.Now()
	}
	r.serviceName, r.methodName = splitMethodName(fullMethod)
	r.reportedMethod = r.methodName
	if guard := r.metrics.cardinalityGuard; guard != nil {
		r.reportedMethod = guard.Resolve(rpcType, r.serviceName, r.methodName)
	}
	r.metrics.serverStartedCounter.WithLabelValues(string(r.rpcType), r.serviceName, r.reportedMethod).Inc()
	return r
}

func (r *serverReporter) ReceivedMessage() {
	r.metrics.serverStreamMsgReceived.WithLabelValues(string(r.rpcType), r.serviceName, r.reportedMethod).Inc()
}

func (r *serverReporter) SentMessage() {
	r.metrics.serverStreamMsgSent.WithLabelValues(string(r.rpcType), r.serviceName, r.reportedMethod).Inc()
}

func (r *serverReporter) Handled(code codes.Code) {
	// Cancelled RPCs abandon their reservation instead of confirming it, so
	// a flood of cancellations can never reserve permanent capacity ahead
	// of real traffic. Every other outcome (success or failure) commits the
	// slot; success/failure totals are recorded either way.
	if guard := r.metrics.cardinalityGuard; guard != nil {
		if code == codes.Canceled {
			guard.Release(r.rpcType, r.serviceName, r.methodName)
		} else {
			guard.Commit(r.rpcType, r.serviceName, r.methodName)
		}
	}
	r.metrics.serverHandledCounter.WithLabelValues(string(r.rpcType), r.serviceName, r.reportedMethod, code.String()).Inc()
	if r.metrics.serverHandledHistogramEnabled {
		r.metrics.serverHandledHistogram.WithLabelValues(string(r.rpcType), r.serviceName, r.reportedMethod).Observe(time.Since(r.startTime).Seconds())
	}
}
