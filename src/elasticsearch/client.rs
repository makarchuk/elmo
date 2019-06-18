use super::queries;
use std::error::Error;

pub struct ElasticClient {
    pub base_url: reqwest::Url,
    pub http_client: reqwest::Client,
}

impl ElasticClient {
    pub fn perform<Q>(&self, q: Q) -> Result<Q::Response, Box<dyn Error>>
    where
        Q: queries::Query,
    {
        let url = self.base_url.join(&q.path())?;
        Ok(self
            .http_client
            .post(url)
            .body(q.payload())
            .header("Content-Type", "application/json")
            .send()?
            .json()?)
    }

    pub fn count_query(
        &self,
        index: &str,
        doc_type: &Option<String>,
        filters: queries::Filters,
    ) -> queries::CountQuery {
        queries::CountQuery {
            index: index.to_string(),
            doc_type: doc_type.clone(),
            filters: filters,
        }
    }
}
