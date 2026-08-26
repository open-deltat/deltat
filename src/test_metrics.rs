//! Log-everything metric recorder shared by the wire and engine metric tests.
//!
//! Every counter increment, gauge set, and histogram sample is kept with its full key, so
//! tests can assert exactly what a code path recorded.

use std::sync::{Arc, Mutex};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};

#[derive(Default)]
pub(crate) struct MetricLog {
    counters: Mutex<Vec<(Key, u64)>>,
    gauges: Mutex<Vec<(Key, f64)>>,
    histograms: Mutex<Vec<(Key, f64)>>,
}

impl MetricLog {
    /// Sum of increments to counter `name`, restricted to entries carrying every label pair.
    pub(crate) fn counter_total(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key_matches(key, name, labels))
            .map(|(_, v)| v)
            .sum()
    }

    /// Every `set` on gauge `name`, in call order.
    pub(crate) fn gauge_sets(&self, name: &str) -> Vec<f64> {
        self.gauges
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name)
            .map(|(_, v)| *v)
            .collect()
    }

    /// Every sample recorded into histogram `name`, in call order.
    pub(crate) fn histogram_values(&self, name: &str) -> Vec<f64> {
        self.histograms
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name)
            .map(|(_, v)| *v)
            .collect()
    }

    pub(crate) fn histogram_recorded(&self, name: &str, labels: &[(&str, &str)]) -> bool {
        self.histograms.lock().unwrap().iter().any(|(key, _)| key_matches(key, name, labels))
    }
}

fn key_matches(key: &Key, name: &str, labels: &[(&str, &str)]) -> bool {
    key.name() == name
        && labels
            .iter()
            .all(|(lk, lv)| key.labels().any(|l| l.key() == *lk && l.value() == *lv))
}

struct RecordingHandle {
    key: Key,
    log: Arc<MetricLog>,
}

impl metrics::CounterFn for RecordingHandle {
    fn increment(&self, value: u64) {
        self.log.counters.lock().unwrap().push((self.key.clone(), value));
    }
    fn absolute(&self, _value: u64) {}
}

impl metrics::GaugeFn for RecordingHandle {
    fn increment(&self, _value: f64) {}
    fn decrement(&self, _value: f64) {}
    fn set(&self, value: f64) {
        self.log.gauges.lock().unwrap().push((self.key.clone(), value));
    }
}

impl metrics::HistogramFn for RecordingHandle {
    fn record(&self, value: f64) {
        self.log.histograms.lock().unwrap().push((self.key.clone(), value));
    }
}

struct TestRecorder(Arc<MetricLog>);

impl Recorder for TestRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(RecordingHandle { key: key.clone(), log: self.0.clone() }))
    }
    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(RecordingHandle { key: key.clone(), log: self.0.clone() }))
    }
    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(RecordingHandle { key: key.clone(), log: self.0.clone() }))
    }
}

/// Run `f` with a fresh local recorder and hand back what it captured. The recorder is
/// thread-local, so async work goes through `block_on` (current-thread runtime) inside `f`.
pub(crate) fn with_metrics<T>(f: impl FnOnce() -> T) -> (Arc<MetricLog>, T) {
    let log = Arc::new(MetricLog::default());
    let out = metrics::with_local_recorder(&TestRecorder(log.clone()), f);
    (log, out)
}

pub(crate) fn block_on<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(fut)
}
