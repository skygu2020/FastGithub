use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

const RATE_WINDOW: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
pub enum FlowDirection {
    Read,
    Write,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowStatistics {
    pub total_read: u64,
    pub total_write: u64,
    pub read_rate: f64,
    pub write_rate: f64,
}

#[derive(Default)]
struct FlowQueue {
    entries: VecDeque<(Instant, u64)>,
}

impl FlowQueue {
    fn record(&mut self, bytes: u64, now: Instant) {
        self.entries.push_back((now, bytes));
        self.clean(now);
    }

    fn rate(&mut self, now: Instant) -> f64 {
        self.clean(now);
        let bytes: u64 = self.entries.iter().map(|(_, bytes)| *bytes).sum();
        bytes as f64 / RATE_WINDOW.as_secs_f64()
    }

    fn clean(&mut self, now: Instant) {
        while self
            .entries
            .front()
            .is_some_and(|(timestamp, _)| now.duration_since(*timestamp) >= RATE_WINDOW)
        {
            self.entries.pop_front();
        }
    }
}

#[derive(Default)]
struct FlowState {
    total_read: AtomicU64,
    total_write: AtomicU64,
    read_queue: Mutex<FlowQueue>,
    write_queue: Mutex<FlowQueue>,
}

/// Shared transport-flow counters and five-second rolling rates.
#[derive(Clone, Default)]
pub struct FlowAnalyzer {
    state: Arc<FlowState>,
}

impl FlowAnalyzer {
    pub fn record(&self, direction: FlowDirection, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let bytes = bytes as u64;
        let now = Instant::now();
        match direction {
            FlowDirection::Read => {
                self.state.total_read.fetch_add(bytes, Ordering::Relaxed);
                if let Ok(mut queue) = self.state.read_queue.lock() {
                    queue.record(bytes, now);
                }
            }
            FlowDirection::Write => {
                self.state.total_write.fetch_add(bytes, Ordering::Relaxed);
                if let Ok(mut queue) = self.state.write_queue.lock() {
                    queue.record(bytes, now);
                }
            }
        }
    }

    pub fn statistics(&self) -> FlowStatistics {
        let now = Instant::now();
        let read_rate = self
            .state
            .read_queue
            .lock()
            .map(|mut queue| queue.rate(now))
            .unwrap_or(0.0);
        let write_rate = self
            .state
            .write_queue
            .lock()
            .map(|mut queue| queue.rate(now))
            .unwrap_or(0.0);

        FlowStatistics {
            total_read: self.state.total_read.load(Ordering::Relaxed),
            total_write: self.state.total_write.load(Ordering::Relaxed),
            read_rate,
            write_rate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_bytes_and_reports_five_second_rates() {
        let analyzer = FlowAnalyzer::default();
        analyzer.record(FlowDirection::Read, 500);
        analyzer.record(FlowDirection::Write, 1_000);
        let stats = analyzer.statistics();
        assert_eq!(stats.total_read, 500);
        assert_eq!(stats.total_write, 1_000);
        assert!((stats.read_rate - 100.0).abs() < 0.001);
        assert!((stats.write_rate - 200.0).abs() < 0.001);
    }
}
