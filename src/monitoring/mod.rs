pub mod sharp_decrease;
use super::alert::Alert;
use super::elasticsearch::queries;
use chrono;

trait Monitoring {}

//Search rescribes search condition for a single monitoring
pub struct Search {
    pub index: String,
    pub doc_type: Option<String>,
    pub filters: Vec<serde_json::Map<String, serde_json::Value>>,
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
