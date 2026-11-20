// Copyright 2016 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
)

type clientReporter struct {
	metrics     *ClientMetrics
	rpcType     grpcType
	serviceName string
	methodName  string
	// reportedMethod is the grpc_method label value actually used on every
	// metric family; it equals methodName unless cardinality protection
	// folded the RPC into unknown.
	reportedMethod string
	startTime      time.Time
}

func newClientReporter(m *ClientMetrics, rpcType grpcType, fullMethod string) *clientReporter {
	r := &clientReporter{
		metrics: m,
		rpcType: rpcType,
	}
	if r.metrics.clientHandledHistogramEnabled {
		r.startTime = time.Now()
	}
	r.serviceName, r.methodName = splitMethodName(fullMethod)
	r.reportedMethod = r.methodName
	if guard := r.metrics.cardinalityGuard; guard != nil {
		r.reportedMethod = guard.Resolve(rpcType, r.serviceName, r.methodName)
	}
	r.metrics.clientStartedCounter.WithLabelValues(string(r.rpcType), r.serviceName, r.reportedMethod).Inc()
	return r
}

// timer is a helper interface to time functions.
type timer interface {
	ObserveDuration() time.Duration
}

type noOpTimer struct {
}

func (noOpTimer) ObserveDuration() time.Duration {
	return 0
}

var emptyTimer = noOpTimer{}

func (r *clientReporter) ReceiveMessageTimer() timer {
	if r.metrics.clientStreamRecvHistogramEnabled {
		hist := r.metrics.clientStreamRecvHistogram.WithLabelValues(string(r.rpcType), r.serviceName, r.reportedMethod)
		return prometheus.NewTimer(hist)
	}

	return emptyTimer
}

func (r *clientReporter) ReceivedMessage() {
	r.metrics.clientStreamMsgReceived.WithLabelValues(string(r.rpcType), r.serviceName, r.reportedMethod).Inc()
}

func (r *clientReporter) SendMessageTimer() timer {
	if r.metrics.clientStreamSendHistogramEnabled {
		hist := r.metrics.clientStreamSendHistogram.WithLabelValues(string(r.rpcType), r.serviceName, r.reportedMethod)
		return prometheus.NewTimer(hist)
	}

	return emptyTimer
}

func (r *clientReporter) SentMessage() {
	r.metrics.clientStreamMsgSent.WithLabelValues(string(r.rpcType), r.serviceName, r.reportedMethod).Inc()
}

func (r *clientReporter) Handled(code codes.Code) {
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
	r.metrics.clientHandledCounter.WithLabelValues(string(r.rpcType), r.serviceName, r.reportedMethod, code.String()).Inc()
	if r.metrics.clientHandledHistogramEnabled {
		r.metrics.clientHandledHistogram.WithLabelValues(string(r.rpcType), r.serviceName, r.reportedMethod).Observe(time.Since(r.startTime).Seconds())
	}
}
