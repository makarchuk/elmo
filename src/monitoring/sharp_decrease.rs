use super::super::alert::Alert;
use super::super::elasticsearch;
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

impl SharpDecrease {
    pub fn check(
        &self,
        client: &elasticsearch::client::ElasticClient,
        point: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<Alert>, Box<Error>> {
        let interval = chrono::Duration::from_std(self.interval).unwrap();
        let filters = elasticsearch::queries::add_filter(
            &self.search.filters,
            super::range_query(
                self.search.time_field.clone(),
                point - interval * (self.look_back + 1).into(),
                point - interval,
            ),
        );
        let old_count = client
            .perform(client.count_query(
                &self.search.index,
                &self.search.doc_type,
                filters.clone(),
            ))?
            .count;
        let filters = elasticsearch::queries::add_filter(
            &self.search.filters,
            super::range_query(self.search.time_field.clone(), point - interval, point),
        );
        let new_count = client
            .perform(client.count_query(
                &self.search.index,
                &self.search.doc_type,
                filters.clone(),
            ))?
            .count;
        let factored_count: u64 = new_count * self.factor as u64 * self.look_back as u64;
        if factored_count < old_count {
            println!(
                "Sharp decrease detected at {}. Was: {}. Now: {}",
                point, old_count, new_count
            );
            return Ok(vec![Alert {}]);
        }
        return Ok(vec![]);
    }
}
