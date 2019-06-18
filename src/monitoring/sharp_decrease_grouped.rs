use super::super::alert::Alert;
use super::super::elasticsearch;
use chrono;
use std::error::Error;
use std::time::Duration;

pub struct SharpDecreaseGrouped {
    // Monitored interval
    pub interval: Duration,
    // We are comparing interval with previous look_back * interval
    // It will help us to ignore short spikes
    pub look_back: u8,
    //decrease factor
    pub factor: u8,

    pub key: String,
    pub groups_count: u32,

    pub search: super::Search,
}

impl SharpDecreaseGrouped {
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
        let old_counts = client
            .perform(elasticsearch::queries::TermsCountQuery {
                doc_type: self.search.doc_type.clone(),
                index: self.search.index.clone(),
                filters: filters,
                key: self.key.clone(),
                count: self.groups_count,
            })?
            .aggregations
            .group_by_key
            .buckets;
        let filters = elasticsearch::queries::add_filter(
            &self.search.filters,
            super::range_query(self.search.time_field.clone(), point - interval, point),
        );
        let new_counts = client
            .perform(elasticsearch::queries::TermsCountQuery {
                doc_type: self.search.doc_type.clone(),
                index: self.search.index.clone(),
                filters: filters,
                key: self.key.clone(),
                count: self.groups_count,
            })?
            .aggregations
            .group_by_key
            .buckets;

        return Ok(vec![]);
    }
}
