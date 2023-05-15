use std::sync::atomic::*;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
struct AddressStatFields {
    xact_count: Arc<AtomicU64>,
    query_count: Arc<AtomicU64>,
    bytes_received: Arc<AtomicU64>,
    bytes_sent: Arc<AtomicU64>,
    xact_time: Arc<AtomicU64>,
    query_time: Arc<AtomicU64>,
    wait_time: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
}

/// Internal address stats
#[derive(Debug, Clone, Default)]
pub struct AddressStats {
    total: AddressStatFields,

    current: AddressStatFields,

    averages: AddressStatFields,

    // Determines if the averages have been updated since the last time they were reported
    pub averages_updated: Arc<AtomicBool>,
}

impl IntoIterator for AddressStats {
    type Item = (String, u64);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        vec![
            (
                "total_xact_count".to_string(),
                self.total.xact_count.load(Ordering::Relaxed),
            ),
            (
                "total_query_count".to_string(),
                self.total.query_count.load(Ordering::Relaxed),
            ),
            (
                "total_received".to_string(),
                self.total.bytes_received.load(Ordering::Relaxed),
            ),
            (
                "total_sent".to_string(),
                self.total.bytes_sent.load(Ordering::Relaxed),
            ),
            (
                "total_xact_time".to_string(),
                self.total.xact_time.load(Ordering::Relaxed),
            ),
            (
                "total_query_time".to_string(),
                self.total.query_time.load(Ordering::Relaxed),
            ),
            (
                "total_wait_time".to_string(),
                self.total.wait_time.load(Ordering::Relaxed),
            ),
            (
                "total_errors".to_string(),
                self.total.errors.load(Ordering::Relaxed),
            ),
            (
                "avg_xact_count".to_string(),
                self.averages.xact_count.load(Ordering::Relaxed),
            ),
            (
                "avg_query_count".to_string(),
                self.averages.query_count.load(Ordering::Relaxed),
            ),
            (
                "avg_recv".to_string(),
                self.averages.bytes_received.load(Ordering::Relaxed),
            ),
            (
                "avg_sent".to_string(),
                self.averages.bytes_sent.load(Ordering::Relaxed),
            ),
            (
                "avg_errors".to_string(),
                self.averages.errors.load(Ordering::Relaxed),
            ),
            (
                "avg_xact_time".to_string(),
                self.averages.xact_time.load(Ordering::Relaxed),
            ),
            (
                "avg_query_time".to_string(),
                self.averages.query_time.load(Ordering::Relaxed),
            ),
            (
                "avg_wait_time".to_string(),
                self.averages.wait_time.load(Ordering::Relaxed),
            ),
        ]
        .into_iter()
    }
}

struct CountStatField {
    current: Arc<AtomicU64>,
    corresponding_stat: Option<Arc<AtomicU64>>,
    average: Arc<AtomicU64>,
}

impl AddressStats {
    pub fn xact_count_add(&self) {
        self.total.xact_count.fetch_add(1, Ordering::Relaxed);
        self.current.xact_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn query_count_add(&self) {
        self.total.query_count.fetch_add(1, Ordering::Relaxed);
        self.current.query_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn bytes_received_add(&self, bytes: u64) {
        self.total
            .bytes_received
            .fetch_add(bytes, Ordering::Relaxed);
        self.current
            .bytes_received
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn bytes_sent_add(&self, bytes: u64) {
        self.total.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        self.current.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn xact_time_add(&self, time: u64) {
        self.total.xact_time.fetch_add(time, Ordering::Relaxed);
        self.current.xact_time.fetch_add(time, Ordering::Relaxed);
    }

    pub fn query_time_add(&self, time: u64) {
        self.total.query_time.fetch_add(time, Ordering::Relaxed);
        self.current.query_time.fetch_add(time, Ordering::Relaxed);
    }

    pub fn wait_time_add(&self, time: u64) {
        self.total.wait_time.fetch_add(time, Ordering::Relaxed);
        self.current.wait_time.fetch_add(time, Ordering::Relaxed);
    }

    pub fn error(&self) {
        self.total.errors.fetch_add(1, Ordering::Relaxed);
        self.current.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_averages(&self) {
        for count_field in self.count_fields_iterator() {
            let current_value = count_field.current.load(Ordering::Relaxed);

            match count_field.corresponding_stat {
                // This means that averaging by time makes sense here
                None => count_field.average.store(
                    current_value / (crate::stats::STAT_PERIOD / 1_000),
                    Ordering::Relaxed,
                ),
                // This means we should average by some corresponding field, ie. number of queries
                Some(corresponding_stat) => {
                    let corresponding_stat_value = corresponding_stat.load(Ordering::Relaxed);
                    count_field
                        .average
                        .store(current_value / corresponding_stat_value, Ordering::Relaxed);
                }
            };
        }

        // Reset current counts to 0
        for count_field in self.count_fields_iterator() {
            count_field.current.store(0, Ordering::Relaxed);
        }
    }

    pub fn populate_row(&self, row: &mut Vec<String>) {
        for (_key, value) in self.clone() {
            row.push(value.to_string());
        }
    }

    fn count_fields_iterator(&self) -> Vec<CountStatField> {
        let mut count_fields: Vec<CountStatField> = Vec::new();

        count_fields.push(CountStatField {
            current: self.current.xact_count.clone(),
            corresponding_stat: None,
            average: self.averages.xact_count.clone(),
        });

        count_fields.push(CountStatField {
            current: self.current.query_count.clone(),
            corresponding_stat: None,
            average: self.averages.query_count.clone(),
        });

        count_fields.push(CountStatField {
            current: self.current.bytes_received.clone(),
            corresponding_stat: None,
            average: self.averages.bytes_received.clone(),
        });

        count_fields.push(CountStatField {
            current: self.current.bytes_sent.clone(),
            corresponding_stat: None,
            average: self.averages.bytes_sent.clone(),
        });

        count_fields.push(CountStatField {
            current: self.current.xact_time.clone(),
            corresponding_stat: Some(self.total.xact_count.clone()),
            average: self.averages.xact_time.clone(),
        });

        count_fields.push(CountStatField {
            current: self.current.query_time.clone(),
            corresponding_stat: Some(self.total.query_count.clone()),
            average: self.averages.query_time.clone(),
        });

        count_fields.push(CountStatField {
            current: self.current.wait_time.clone(),
            corresponding_stat: None,
            average: self.averages.wait_time.clone(),
        });

        count_fields.push(CountStatField {
            current: self.current.errors.clone(),
            corresponding_stat: None,
            average: self.averages.errors.clone(),
        });

        count_fields
    }
}
