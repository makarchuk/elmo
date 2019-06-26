// pub mod sharp_decrease;
// pub mod sharp_decrease_grouped;
use super::alert::Alert;
use super::elasticsearch::queries;
use chrono;
use std::collections::{HashMap, HashSet};
use std::error::Error;

pub trait IntoMonitoring {
    type Monitoring;

    fn into_monitoring(self) -> Self::Monitoring;
}

trait Monitoring {
    type Metric;

    //Load metric from Elasticsearch. Hardly testable, and very fragile
    fn load_metric(
        &self,
        client: &super::elasticsearch::client::ElasticClient,
        point: chrono::DateTime<chrono::Utc>,
    ) -> Result<Self::Metric, Box<Error>>;

    //Check loaded metric for alertability
    fn check(&self, m: Self::Metric) -> Vec<Alert>;
}

struct SharpDecreaseConfig {
    search: Search,
    // Monitored interval
    pub interval: chrono::Duration,
    pub time_factor: u8,
    //decrease factor
    pub factor: u8,
}

struct SharpDecrease {
    time_factor: u8,
    search: Search,
    comparartor: Comparator,
    interval: chrono::Duration,
}

struct Comparator {
    factor: u8,
    time_factor: u8,
}

impl Comparator {
    fn check(&self, diff: &Diff<u64>) -> bool {
        return diff.old > diff.new * (self.time_factor * self.factor) as u64;
    }
}

struct Diff<T> {
    old: T,
    new: T,
}

impl SharpDecrease {
    fn get_count(
        &self,
        client: &super::elasticsearch::client::ElasticClient,
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, Box<Error>> {
        Ok(client
            .perform(client.count_query(
                &self.search.index,
                &self.search.doc_type,
                super::elasticsearch::queries::add_filter(
                    &self.search.filters,
                    range_query(self.search.time_field.clone(), from, to),
                ),
            ))?
            .count)
    }
}

impl IntoMonitoring for SharpDecreaseConfig {
    type Monitoring = SharpDecrease;
    fn into_monitoring(self) -> Self::Monitoring {
        Self::Monitoring {
            search: self.search,
            time_factor: self.time_factor,
            comparartor: Comparator {
                time_factor: self.time_factor,
                factor: self.factor,
            },
            interval: self.interval,
        }
    }
}

impl Monitoring for SharpDecrease {
    type Metric = Diff<u64>;

    fn load_metric(
        &self,
        client: &super::elasticsearch::client::ElasticClient,
        point: chrono::DateTime<chrono::Utc>,
    ) -> Result<Self::Metric, Box<Error>> {
        Ok(Diff::<u64> {
            old: self.get_count(
                client,
                point - self.interval * (self.time_factor + 1) as i32,
                point - self.interval,
            )?,
            new: self.get_count(client, point - self.interval, point)?,
        })
    }

    fn check(&self, m: Self::Metric) -> Vec<Alert> {
        if self.comparartor.check(&m) {
            println!("Sharp decrease: {} to {}", m.old, m.new);
            return vec![Alert {}];
        }
        return vec![];
    }
}

struct GroupedSharpDecreaseConfig {
    search: Search,
    // Monitored interval
    pub interval: chrono::Duration,
    pub time_factor: u8,
    //decrease factor
    pub factor: u8,
    pub key_field: String,
    pub groups_count: u8,
}

struct GroupedSharpDecrease {
    time_factor: u8,
    search: Search,
    comparartor: Comparator,
    interval: chrono::Duration,
    key_field: String,
    groups_count: u8,
}

impl GroupedSharpDecrease {
    fn get_counts(
        &self,
        client: &super::elasticsearch::client::ElasticClient,
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> Result<HashMap<String, u64>, Box<Error>> {
        Ok(client
            .perform(super::elasticsearch::queries::TermsCountQuery {
                index: self.search.index.clone(),
                key: self.key_field.clone(),
                doc_type: self.search.doc_type.clone(),
                count: self.groups_count as u32,
                filters: super::elasticsearch::queries::add_filter(
                    &self.search.filters,
                    range_query(self.search.time_field.clone(), from, to),
                ),
            })?
            .aggregations
            .group_by_key
            .buckets
            .into_iter()
            .map(|bucket| (bucket.key, bucket.doc_count))
            .collect())
    }
}

impl IntoMonitoring for GroupedSharpDecreaseConfig {
    type Monitoring = GroupedSharpDecrease;
    fn into_monitoring(self) -> Self::Monitoring {
        unimplemented!();
    }
}

impl Monitoring for GroupedSharpDecrease {
    type Metric = Diff<HashMap<String, u64>>;

    fn load_metric(
        &self,
        client: &super::elasticsearch::client::ElasticClient,
        point: chrono::DateTime<chrono::Utc>,
    ) -> Result<Self::Metric, Box<Error>> {
        Ok(Diff::<_> {
            old: self.get_counts(
                client,
                point - self.interval * (self.time_factor + 1) as i32,
                point - self.interval,
            )?,
            new: self.get_counts(client, point - self.interval, point)?,
        })
    }

    fn check(&self, m: Self::Metric) -> Vec<Alert> {
        m.old
            .keys()
            .into_iter()
            .chain(m.new.keys().into_iter())
            .filter_map(|k| {
                let diff = Diff::<u64> {
                    old: *m.old.get(k).unwrap_or(&0),
                    new: *m.new.get(k).unwrap_or(&0),
                };
                if self.comparartor.check(&diff) {
                    println!(
                        "Sharp decrease: {} to {} for key: {}",
                        diff.old, diff.new, k
                    );
                    Some(Alert {})
                } else {
                    None
                }
            })
            .collect()
    }
}

//Search rescribes search condition for a single monitoring
pub struct Search {
    pub index: String,
    pub doc_type: Option<String>,
    pub filters: super::elasticsearch::queries::Filters,
    pub time_field: String,
}

pub fn range_query(
    field: String,
    from: chrono::DateTime<chrono::Utc>,
    to: chrono::DateTime<chrono::Utc>,
) -> queries::Filter {
    let mut range = serde_json::Map::new();
    range.insert(
        "gte".to_owned(),
        serde_json::Value::String(from.to_rfc3339()),
    );
    range.insert("lte".to_owned(), serde_json::Value::String(to.to_rfc3339()));
    let mut outer_range = serde_json::Map::new();
    outer_range.insert(field, serde_json::Value::Object(range));
    let mut result = serde_json::Map::new();
    result.insert("range".to_owned(), serde_json::Value::Object(outer_range));
    result
}
