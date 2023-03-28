use log::warn;
use std::sync::atomic::*;
use std::sync::Arc;

/// Internal address stats
#[derive(Debug, Clone, Default)]
pub struct AddressStats {
    pub total_xact_count: Arc<AtomicU64>,
    pub total_query_count: Arc<AtomicU64>,
    pub total_received: Arc<AtomicU64>,
    pub total_sent: Arc<AtomicU64>,
    pub total_xact_time: Arc<AtomicU64>,
    pub total_query_time: Arc<AtomicU64>,
    pub total_wait_time: Arc<AtomicU64>,
    pub total_errors: Arc<AtomicU64>,
    pub avg_query_count: Arc<AtomicU64>,
    pub avg_query_time: Arc<AtomicU64>,
    pub avg_recv: Arc<AtomicU64>,
    pub avg_sent: Arc<AtomicU64>,
    pub avg_errors: Arc<AtomicU64>,
    pub avg_xact_time: Arc<AtomicU64>,
    pub avg_xact_count: Arc<AtomicU64>,
    pub avg_wait_time: Arc<AtomicU64>,
}

impl IntoIterator for AddressStats {
    type Item = (String, u64);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        vec![
            (
                "total_xact_count".to_string(),
                self.total_xact_count.load(Ordering::Relaxed),
            ),
            (
                "total_query_count".to_string(),
                self.total_query_count.load(Ordering::Relaxed),
            ),
            (
                "total_received".to_string(),
                self.total_received.load(Ordering::Relaxed),
            ),
            (
                "total_sent".to_string(),
                self.total_sent.load(Ordering::Relaxed),
            ),
            (
                "total_xact_time".to_string(),
                self.total_xact_time.load(Ordering::Relaxed),
            ),
            (
                "total_query_time".to_string(),
                self.total_query_time.load(Ordering::Relaxed),
            ),
            (
                "total_wait_time".to_string(),
                self.total_wait_time.load(Ordering::Relaxed),
            ),
            (
                "total_errors".to_string(),
                self.total_errors.load(Ordering::Relaxed),
            ),
            (
                "avg_xact_count".to_string(),
                self.avg_xact_count.load(Ordering::Relaxed),
            ),
            (
                "avg_query_count".to_string(),
                self.avg_query_count.load(Ordering::Relaxed),
            ),
            (
                "avg_recv".to_string(),
                self.avg_recv.load(Ordering::Relaxed),
            ),
            (
                "avg_sent".to_string(),
                self.avg_sent.load(Ordering::Relaxed),
            ),
            (
                "avg_errors".to_string(),
                self.avg_errors.load(Ordering::Relaxed),
            ),
            (
                "avg_xact_time".to_string(),
                self.avg_xact_time.load(Ordering::Relaxed),
            ),
            (
                "avg_query_time".to_string(),
                self.avg_query_time.load(Ordering::Relaxed),
            ),
            (
                "avg_wait_time".to_string(),
                self.avg_wait_time.load(Ordering::Relaxed),
            ),
        ]
        .into_iter()
    }
}

impl AddressStats {
    pub fn error(&self) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_averages(&self) {
        let (totals, averages) = self.fields_iterators();
        for data in totals.iter().zip(averages.iter()) {
            let (total, average) = data;
            if let Err(err) = average.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |avg| {
                let total = total.load(Ordering::Relaxed);
                let avg = (total - avg) / (crate::stats::STAT_PERIOD / 1_000); // Avg / second
                Some(avg)
            }) {
                warn!("Could not update averages for addresses stats, {:?}", err);
            }
        }
    }

    pub fn populate_row(&self, row: &mut Vec<String>) {
        for (_key, value) in self.clone() {
            row.push(value.to_string());
        }
    }

    fn fields_iterators(&self) -> (Vec<Arc<AtomicU64>>, Vec<Arc<AtomicU64>>) {
        let mut totals: Vec<Arc<AtomicU64>> = Vec::new();
        let mut averages: Vec<Arc<AtomicU64>> = Vec::new();

        totals.push(self.total_xact_count.clone());
        averages.push(self.avg_xact_count.clone());
        totals.push(self.total_query_count.clone());
        averages.push(self.avg_query_count.clone());
        totals.push(self.total_received.clone());
        averages.push(self.avg_recv.clone());
        totals.push(self.total_sent.clone());
        averages.push(self.avg_sent.clone());
        totals.push(self.total_xact_time.clone());
        averages.push(self.avg_xact_time.clone());
        totals.push(self.total_query_time.clone());
        averages.push(self.avg_query_time.clone());
        totals.push(self.total_wait_time.clone());
        averages.push(self.avg_wait_time.clone());
        totals.push(self.total_errors.clone());
        averages.push(self.avg_errors.clone());

        (totals, averages)
    }
}
