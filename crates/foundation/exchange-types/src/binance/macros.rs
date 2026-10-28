/// Generates the `rate_limit_usage` accessor for a Binance request.
macro_rules! binance_rate_limit_usage {
    (arms { $($rate_arms:tt)* }) => {
        fn rate_limit_usage(
            &self,
            restriction: crate::rate_limited::RateLimitRestriction,
        ) -> crate::new_types::UsageCount {
            match restriction {
                $($rate_arms)*
            }
        }
    };
    (method $method:ident) => {
        fn rate_limit_usage(
            &self,
            restriction: crate::rate_limited::RateLimitRestriction,
        ) -> crate::new_types::UsageCount {
            self.$method(restriction)
        }
    };
}

/// Implements [`ETRequest`](crate::request::ETRequest) for a Binance request.
///
/// The shared accessor scaffolding is generated from the request's signedness,
/// idempotency and rate-limit behaviour. Query parameter serialization is
/// delegated to the request's `binance_query_params` helper, which the request
/// construction macros generate from the `QueryParams` derive.
macro_rules! impl_binance_request {
    (
        signed $name:ty,
        idempotent: $idempotent:expr,
        rate_limit: { $($rate_arms:tt)* }
    ) => {
        impl crate::request::ETRequest for $name {
            type Exchange = crate::binance::exchange::BinanceExchange;

            fn is_signed(&self) -> bool {
                true
            }

            fn is_idempotent(&self) -> bool {
                $idempotent
            }

            crate::binance::macros::binance_rate_limit_usage!(arms { $($rate_arms)* });

            fn set_api_key(&mut self, api_key: Option<String>) {
                self.apiKey = api_key;
            }

            fn set_timestamp(&mut self, timestamp: crate::new_types::TimestampMilliseconds) {
                self.timestamp = timestamp.0;
            }

            fn query_params(&self, percent_encode: bool) -> String {
                Self::binance_query_params(self, percent_encode)
            }
        }
    };
    (
        signed $name:ty,
        idempotent: $idempotent:expr,
        rate_limit_method: $rate_limit_method:ident
    ) => {
        impl crate::request::ETRequest for $name {
            type Exchange = crate::binance::exchange::BinanceExchange;

            fn is_signed(&self) -> bool {
                true
            }

            fn is_idempotent(&self) -> bool {
                $idempotent
            }

            crate::binance::macros::binance_rate_limit_usage!(method $rate_limit_method);

            fn set_api_key(&mut self, api_key: Option<String>) {
                self.apiKey = api_key;
            }

            fn set_timestamp(&mut self, timestamp: crate::new_types::TimestampMilliseconds) {
                self.timestamp = timestamp.0;
            }

            fn query_params(&self, percent_encode: bool) -> String {
                Self::binance_query_params(self, percent_encode)
            }
        }
    };
    (
        unsigned $name:ty,
        idempotent: $idempotent:expr,
        rate_limit: { $($rate_arms:tt)* }
    ) => {
        impl crate::request::ETRequest for $name {
            type Exchange = crate::binance::exchange::BinanceExchange;

            fn is_signed(&self) -> bool {
                false
            }

            fn is_idempotent(&self) -> bool {
                $idempotent
            }

            crate::binance::macros::binance_rate_limit_usage!(arms { $($rate_arms)* });

            fn set_api_key(&mut self, _api_key: Option<String>) {}

            fn set_timestamp(&mut self, _timestamp: crate::new_types::TimestampMilliseconds) {}

            fn query_params(&self, percent_encode: bool) -> String {
                Self::binance_query_params(self, percent_encode)
            }
        }
    };
    (
        unsigned_with_api_key $name:ty,
        idempotent: $idempotent:expr,
        rate_limit: { $($rate_arms:tt)* }
    ) => {
        impl crate::request::ETRequest for $name {
            type Exchange = crate::binance::exchange::BinanceExchange;

            fn is_signed(&self) -> bool {
                false
            }

            fn is_idempotent(&self) -> bool {
                $idempotent
            }

            crate::binance::macros::binance_rate_limit_usage!(arms { $($rate_arms)* });

            fn set_api_key(&mut self, api_key: Option<String>) {
                if api_key.is_some() {
                    self.api_key = api_key;
                }
            }

            fn set_timestamp(&mut self, _timestamp: crate::new_types::TimestampMilliseconds) {}

            fn query_params(&self, percent_encode: bool) -> String {
                Self::binance_query_params(self, percent_encode)
            }
        }
    };
}

/// Defines a signed Binance request.
///
/// The common `apiKey`, `timestamp` and `recvWindow` fields are injected into
/// the struct so that every signed request shares the same wire layout, and the
/// matching `ETRequest` implementation is generated automatically.
macro_rules! binance_signed_request {
    (
        $(#[$meta:meta])*
        pub struct $name:ident {
            $($fields:tt)*
        }
        idempotent: $idempotent:expr,
        rate_limit: { $($rate_arms:tt)* }
    ) => {
        $(#[$meta])*
        pub struct $name {
            apiKey: Option<String>,
            timestamp: i64,
            $($fields)*
            pub recvWindow: Option<crate::binance::recv_window::BinanceRecvWindow>,
        }

        impl $name {
            fn binance_query_params(&self, percent_encode: bool) -> String {
                self.query_params(true, percent_encode)
            }
        }

        crate::binance::macros::impl_binance_request!(
            signed $name,
            idempotent: $idempotent,
            rate_limit: { $($rate_arms)* }
        );
    };
    (
        $(#[$meta:meta])*
        pub struct $name:ident {
            $($fields:tt)*
        }
        idempotent: $idempotent:expr,
        rate_limit_method: $rate_limit_method:ident
    ) => {
        $(#[$meta])*
        pub struct $name {
            apiKey: Option<String>,
            timestamp: i64,
            $($fields)*
            pub recvWindow: Option<crate::binance::recv_window::BinanceRecvWindow>,
        }

        impl $name {
            fn binance_query_params(&self, percent_encode: bool) -> String {
                self.query_params(true, percent_encode)
            }
        }

        crate::binance::macros::impl_binance_request!(
            signed $name,
            idempotent: $idempotent,
            rate_limit_method: $rate_limit_method
        );
    };
}

/// Defines an unsigned Binance request together with its `ETRequest`
/// implementation.
macro_rules! binance_unsigned_request {
    (
        $(#[$meta:meta])*
        pub struct $name:ident {
            $($fields:tt)*
        }
        idempotent: $idempotent:expr,
        rate_limit: { $($rate_arms:tt)* }
    ) => {
        $(#[$meta])*
        pub struct $name {
            $($fields)*
        }

        impl $name {
            fn binance_query_params(&self, percent_encode: bool) -> String {
                self.query_params(true, percent_encode)
            }
        }

        crate::binance::macros::impl_binance_request!(
            unsigned $name,
            idempotent: $idempotent,
            rate_limit: { $($rate_arms)* }
        );
    };
}

pub(crate) use binance_rate_limit_usage;
pub(crate) use binance_signed_request;
pub(crate) use binance_unsigned_request;
pub(crate) use impl_binance_request;
