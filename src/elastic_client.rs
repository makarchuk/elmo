use super::query;
use std::error::Error;

pub struct ElasticClient {
    base_url: reqwest::Url,
    http_client: reqwest::Client,
}

impl ElasticClient {
    pub fn perform<Q>(&self, q: Q) -> Result<Q::Response, Box<dyn Error>>
    where
        Q: query::Query,
    {
        let url = self.base_url.join(&q.path())?;
        let request = self
            .http_client
            .request(reqwest::Method::POST, url)
            .body(q.payload())
            .header("Content-Type", "application/json")
            .build()?;
        Ok(self.http_client.execute(request)?.json()?)
    }
}
