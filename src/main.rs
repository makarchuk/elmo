mod alert;
mod elasticsearch;
mod monitoring;
use chrono;
use reqwest;
use std::thread::sleep;
use std::time::Duration;

fn main() {
    let monitoring = monitoring::sharp_decrease::SharpDecrease {
        interval: Duration::from_secs(600),
        search: monitoring::Search {
            index: "index-template-*".to_owned(),
            doc_type: Some("type_name".to_owned()),
            filters: vec![],
            time_field: "@timestamp".to_owned(),
        },
        look_back: 5,
        factor: 2,
    };
    let client = elasticsearch::client::ElasticClient {
        base_url: "http://elasticsearc.local:9200/".parse().unwrap(),
        http_client: reqwest::Client::new(),
    };
    let mut point = chrono::Utc::now() - chrono::Duration::days(1);
    loop {
        if point < chrono::Utc::now() {
            match monitoring.check(&client, point) {
                Ok(_) => {
                    point = point + chrono::Duration::minutes(10);
                }
                Err(e) => {
                    println!("Error while performing a check: {}", e);
                    sleep(Duration::from_secs(3));
                }
            }
        } else {
            sleep(Duration::from_secs(30));
        }
    }
}
