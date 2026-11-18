use super::{Page, PageQuery};
use crate::api::ApiRequest;
use hyper::{HeaderMap, Method, Uri, body::Bytes};

fn request(query: &str) -> ApiRequest {
    let uri: Uri = format!("http://localhost/v1/bots{query}")
        .parse()
        .expect("uri");
    ApiRequest::from_parts(
        Method::GET,
        uri,
        HeaderMap::new(),
        Bytes::new(),
        None,
        hyper::http::Extensions::new(),
    )
}

fn parse(query: &str) -> Result<PageQuery, crate::api::Problem> {
    PageQuery::parse(&request(query))
}

#[test]
fn pagination_defaults_to_fifty_items() {
    let query = parse("").expect("default query");
    assert_eq!(query.limit(), 50);
    assert_eq!(query.offset(), 0);
}

#[test]
fn limit_is_capped_at_two_hundred() {
    let query = parse("?limit=500").expect("capped query");
    assert_eq!(query.limit(), 200);
}

#[test]
fn a_zero_limit_is_rejected() {
    let problem = parse("?limit=0").expect_err("zero limit must fail");
    assert_eq!(problem.status(), hyper::StatusCode::BAD_REQUEST);
}

#[test]
fn malformed_pagination_parameters_are_rejected() {
    for query in ["?limit=banana", "?limit=-1", "?cursor=banana"] {
        let problem = parse(query).expect_err("malformed query must fail");
        assert_eq!(problem.status(), hyper::StatusCode::BAD_REQUEST);
    }
}

#[test]
fn a_valid_cursor_and_limit_are_accepted() {
    let query = parse("?limit=10&cursor=20").expect("valid query");
    assert_eq!(query.limit(), 10);
    assert_eq!(query.offset(), 20);
}

#[test]
fn pages_link_to_the_next_page() {
    let items: Vec<u8> = (0..3).collect();
    let first = Page::of(items.clone(), &parse("?limit=2").expect("query"));
    assert_eq!(first.data, vec![0, 1]);
    assert_eq!(first.next_cursor.as_deref(), Some("2"));
    let second = Page::of(items, &parse("?limit=2&cursor=2").expect("query"));
    assert_eq!(second.data, vec![2]);
    assert!(second.next_cursor.is_none());
}
