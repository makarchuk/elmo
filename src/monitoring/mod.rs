// pub mod sharp_decrease;
// pub mod sharp_decrease_grouped;
use super::alert::Alert;
use super::elasticsearch::queries;
use chrono;
use std::error::Error;

trait Monitoring {
    type Config;
    type Metric;

    //Initialize monitoring from configuartion
    fn new(c: Self::Config) -> Self;

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

impl Monitoring for SharpDecrease {
    type Config = SharpDecreaseConfig;
    type Metric = Diff<u64>;

    fn new(config: Self::Config) -> Self {
        return Self {
            search: config.search,
            time_factor: config.time_factor,
            comparartor: Comparator {
                time_factor: config.time_factor,
                factor: config.factor,
            },
            interval: config.interval,
        };
    }

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
