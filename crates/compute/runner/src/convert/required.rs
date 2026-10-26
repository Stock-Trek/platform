use stock_trek::errors::{StockTrekError, StockTrekResult, ValueError};

pub trait IsRequired {
    type Output;
    fn required(self, field_name: &str) -> StockTrekResult<Self::Output>;
}

impl<T> IsRequired for Option<T> {
    type Output = T;

    fn required(self, field_name: &str) -> StockTrekResult<T> {
        self.ok_or_else(|| {
            StockTrekError::Value(ValueError::NotFound {
                name: field_name.to_string(),
                key: field_name.to_string(),
            })
        })
    }
}
