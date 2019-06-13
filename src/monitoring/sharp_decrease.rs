//Not imported

use super::super::alert::Alert;
use super::super::elasticsearch;
use super::Comparator;
use chrono;
use std::error::Error;
use std::time::Duration;

pub struct SharpDecrease {
    // Monitored interval
    pub interval: Duration,
    // We are comparing interval with previous look_back * interval
    // It will help us to ignore short spikes
    pub look_back: u8,
    //decrease factor
    pub factor: u8,

    pub search: super::Search,
}

struct Metric {
    old: u64,
    new: u64,
}

struct SharpDecreaseComparator {
    factor: u8,
    look_back: u8,
}

impl SharpDecreaseComparator {
    fn new(factor: u8, look_back: u8) -> Self {
        Self { factor, look_back }
    }

    fn check(&self, metric: Self::Metric) -> Vec<Alert> {
        let factored_count: u64 = metric.new * self.factor as u64 * self.look_back as u64;
        if factored_count < metric.old {
            return vec![Alert {}];
        }
        vec![]
    }
}

impl SharpDecrease {
    pub fn check(
        &self,
        client: &elasticsearch::client::ElasticClient,
        point: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<Alert>, Box<Error>> {
        let interval = chrono::Duration::from_std(self.interval).unwrap();
        let old_count = client
            .perform(client.count_query(
                &self.search.index,
                &self.search.doc_type,
                elasticsearch::queries::add_filter(
                    &self.search.filters,
                    super::range_query(
                        self.search.time_field.clone(),
                        point - interval * (self.look_back + 1).into(),
                        point - interval,
                    ),
                ),
            ))?
            .count;
        let new_count = client
            .perform(client.count_query(
                &self.search.index,
                &self.search.doc_type,
                elasticsearch::queries::add_filter(
                    &self.search.filters,
                    super::range_query(self.search.time_field.clone(), point - interval, point),
                ),
            ))?
            .count;
        Ok(
            SharpDecreaseComparator::new(self.factor, self.look_back).check(Metric {
                new: new_count,
                old: old_count,
            }),
        )
    }
}
