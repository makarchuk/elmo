use itertools::Itertools;
use serde_json;

pub trait Query {
    type Response: serde::de::DeserializeOwned;

    fn payload(&self) -> Vec<u8>;

    fn path(&self) -> String;
}

pub type Filter = serde_json::Map<String, serde_json::Value>;
pub type Filters = Vec<Filter>;

//Helper function that allows us to push additional filter
pub fn add_filter(base: &Filters, additional: Filter) -> Filters {
    let mut new_filters = base.clone();
    new_filters.push(additional);
    new_filters
}

pub struct CountQuery {
    pub index: String,
    pub doc_type: Option<String>,
    pub filters: Filters,
}

#[derive(serde::Deserialize)]
pub struct CountQueryResponse {
    pub count: u64,
}

impl Query for CountQuery {
    type Response = CountQueryResponse;

    fn payload(&self) -> Vec<u8> {
        #[derive(serde::Serialize)]
        struct Request {
            query: InternalQuery,
        }
        let request_body = Request {
            query: InternalQuery {
                bool: InternalFilters {
                    filter: self.filters.clone(),
                },
            },
        };
        serde_json::to_vec(&request_body).unwrap()
    }

    fn path(&self) -> String {
        let mut parts: Vec<&str> = Vec::new();
        parts.push(&self.index);
        if let Some(doc_type) = &self.doc_type {
            parts.push(doc_type)
        }
        parts.push("_count");
        parts.iter().join("/")
    }
}

pub struct TermsCountQuery {
    pub index: String,
    pub doc_type: Option<String>,
    pub filters: Filters,

    pub key: String,
    pub count: u32,
}

#[derive(serde::Deserialize)]
pub struct TermsCountQueryResponse {
    pub aggregations: Aggregations,
}

#[derive(serde::Deserialize)]
pub struct Aggregations {
    pub group_by_key: GroupByKey,
}

#[derive(serde::Deserialize)]
pub struct Bucket {
    pub key: String,
    pub doc_count: u64,
}

#[derive(serde::Deserialize)]
pub struct GroupByKey {
    pub doc_count_error_upper_bound: u64,
    pub sum_other_doc_count: u64,
    pub buckets: Vec<Bucket>,
}

impl Query for TermsCountQuery {
    type Response = TermsCountQueryResponse;

    fn payload(&self) -> Vec<u8> {
        #[derive(serde::Serialize)]
        struct Request {
            query: InternalQuery,
            //u8 is enough. I'm planning to use 0 anyway
            size: u8,
            aggregations: RequestAggregations,
        }

        #[derive(serde::Serialize)]
        struct RequestAggregations {
            group_by_key: TermsAggregation,
        }

        #[derive(serde::Serialize)]
        struct TermsAggregation {
            terms: RequstTermsBody,
        }

        #[derive(serde::Serialize)]
        struct RequstTermsBody {
            field: String,
            count: u32,
        }
        let request_body = Request {
            size: 0,
            query: InternalQuery {
                bool: InternalFilters {
                    filter: self.filters.clone(),
                },
            },
            aggregations: RequestAggregations {
                group_by_key: TermsAggregation {
                    terms: RequstTermsBody {
                        field: self.key.clone(),
                        count: self.count,
                    },
                },
            },
        };
        serde_json::to_vec(&request_body).unwrap()
    }

    fn path(&self) -> String {
        let mut parts: Vec<&str> = Vec::new();
        parts.push(&self.index);
        if let Some(doc_type) = &self.doc_type {
            parts.push(doc_type)
        }
        parts.push("_search");
        parts.iter().join("/")
    }
}

#[derive(serde::Serialize)]
struct InternalQuery {
    bool: InternalFilters,
}

#[derive(serde::Serialize)]
struct InternalFilters {
    filter: Filters,
}
