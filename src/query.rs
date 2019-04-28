use itertools::Itertools;
use serde_json;
use std::error::Error;

trait Query {
    type Response: serde::de::DeserializeOwned;

    // Probably it'll be unnecessary to redefine it ever
    fn execute(&self, cli: reqwest::Client) -> Result<Self::Response, Box<dyn Error>> {
        return Ok(cli.execute(self.request(&cli))?.json()?);
    }

    fn request(&self, cli: &reqwest::Client) -> reqwest::Request;
}

struct CountQuery {
    base_url: reqwest::Url,
    index: String,
    doc_type: Option<String>,
    filters: Vec<serde_json::Map<String, serde_json::Value>>,
}

#[derive(serde::Deserialize)]
struct CountQueryResponse {
    count: u64,
    shards: Shards,
}

impl Query for CountQuery {
    type Response = CountQueryResponse;

    fn request(&self, cli: &reqwest::Client) -> reqwest::Request {
        #[derive(serde::Serialize)]
        struct Request {
            query: InternalQuery,
        }

        let mut url = self.base_url.clone();
        url = url.join(&self.index).unwrap();
        if let Some(doc_type) = &self.doc_type {
            url = url.join(&doc_type).unwrap();
        }
        url = url.join("_count").unwrap();
        cli.post(url)
            .json(&Request {
                query: InternalQuery {
                    bool: self.filters.clone(),
                },
            })
            .build()
            .unwrap()
    }
}

struct TermsCountQuery {
    base_url: reqwest::Url,
    index: String,
    doc_type: Option<String>,
    filters: Vec<serde_json::Map<String, serde_json::Value>>,

    key: String,
    count: u32,
}

#[derive(serde::Deserialize)]
struct TermsCountQueryResponse {
    aggregations: Aggregations,
    shards: Shards,
}

#[derive(serde::Deserialize)]
struct Aggregations {
    group_by_key: GroupByKey,
}

#[derive(serde::Deserialize)]
struct Buckets {
    key: String,
    doc_count: i64,
}

#[derive(serde::Deserialize)]
struct GroupByKey {
    doc_count_error_upper_bound: i64,
    sum_other_doc_count: i64,
    buckets: Vec<Buckets>,
}

impl Query for TermsCountQuery {
    type Response = TermsCountQueryResponse;

    fn request(&self, cli: &reqwest::Client) -> reqwest::Request {
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

        let mut url = self.base_url.clone();
        url = url.join(&self.index).unwrap();
        if let Some(doc_type) = &self.doc_type {
            url = url.join(&doc_type).unwrap();
        }
        url = url.join("_count").unwrap();
        cli.post(url)
            .json(&Request {
                size: 0,
                query: InternalQuery {
                    bool: self.filters.clone(),
                },
                aggregations: RequestAggregations {
                    group_by_key: TermsAggregation {
                        terms: RequstTermsBody {
                            field: self.key.clone(),
                            count: self.count,
                        },
                    },
                },
            })
            .build()
            .unwrap()
    }
}

#[derive(serde::Serialize)]
struct InternalQuery {
    bool: Vec<serde_json::Map<String, serde_json::Value>>,
}

#[derive(serde::Deserialize)]
struct Shards {
    total: u64,
    successful: u64,
    skipped: u64,
    failed: u64,
}
