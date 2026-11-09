package controller

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	kuberikcomv1alpha1 "github.com/kuberik/release-controller/api/v1alpha1"
)

// Metrics (design §4). Per-source series are labelled kind/namespace/name and
// removed when the ReleaseSource is gone.
var (
	syncDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "release_controller_sync_duration_seconds",
		Help:    "Duration of one ReleaseSource sync.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2.5, 10),
	}, []string{"kind"})
	releasesGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "release_controller_releases",
		Help: "Releases in the catalogue of a source.",
	}, []string{"kind", "namespace", "name"})
	slicesGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "release_controller_slices",
		Help: "ReleaseSlices of a source.",
	}, []string{"kind", "namespace", "name"})
	resolveErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "release_controller_resolve_errors_total",
		Help: "Tags whose metadata could not be resolved or was rejected.",
	}, []string{"kind", "namespace", "name"})
)

func init() {
	metrics.Registry.MustRegister(syncDuration, releasesGauge, slicesGauge, resolveErrors)
}

func sourceKind(rs *kuberikcomv1alpha1.ReleaseSource) string {
	if rs.Spec.OCI != nil {
		return "oci"
	}
	return "unknown"
}

func observeSync(rs *kuberikcomv1alpha1.ReleaseSource, start time.Time) {
	syncDuration.WithLabelValues(sourceKind(rs)).Observe(time.Since(start).Seconds())
}

func forgetMetrics(ns, name string) {
	releasesGauge.DeletePartialMatch(map[string]string{"namespace": ns, "name": name})
	slicesGauge.DeletePartialMatch(map[string]string{"namespace": ns, "name": name})
	resolveErrors.DeletePartialMatch(map[string]string{"namespace": ns, "name": name})
}
