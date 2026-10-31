# Stock Trek Types

Shared type definitions for Stock Trek, a multi-exchange crypto trading system.

This crate provides canonical Rust types for representing common cryptocurrency exchange (CEX) concepts — such as orders, trading pairs, pricing, activation, and asset identifiers — used across Stock Trek services.

[![crates.io](https://img.shields.io/crates/v/stock-trek-types)](https://crates.io/crates/stock-trek-types)
[![docs.rs](https://img.shields.io/docsrs/stock-trek-types)](https://docs.rs/stock-trek-types)

## Usage

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
stock-trek-types = "x.y.z"
```

Serde support is optional and off by default. Enable the `serde` feature to get `Serialize`/`Deserialize` implementations for all types

```toml
[dependencies]
stock-trek-types = { version = "x.y.z", features = ["serde"] }
```

## Modules

| Module                                                                           | Description                                                             |
|----------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| [`cex`](https://docs.rs/stock-trek-types/latest/stock_trek_types/cex/index.html) | Centralized exchange types — orders, pairs, pricing, activation, assets |

### `cex`

The `cex` module contains the core domain types:

| Type                          | Description                                                                                                |
|-------------------------------|------------------------------------------------------------------------------------------------------------|
| `AssetId`                     | Supported cryptocurrency assets (Bitcoin, Ethereum, Solana, etc.)                                          |
| `CexId`                       | Supported exchange identifiers (e.g., Binance)                                                             |
| `Side`                        | Order side — `Buy` or `Sell`                                                                               |
| `Status`                      | Order status lifecycle — `New`, `Open`, `PartiallyFilled`, `Filled`, `Canceled`, `Rejected`, `Expired`     |
| `TimeInForce`                 | Time-in-force policies — `GoodTillCancelled`, `FillOrKill`, `ImmediateOrCancel`                            |
| `TradingPair`                 | A base/quote trading pair (e.g., BTC/USDT)                                                                 |
| `Quantity<N>`                 | Order quantity expressed as either `OfBase` or `OfQuote`                                                   |
| `Pricing<N>`                  | Pricing scheme — `Market` or `Limit` (with price and time-in-force)                                        |
| `Activation<N>`               | Order activation condition — `Immediate`, `PriceTriggered`, or `Trailing`                                  |
| `PriceBasis`                  | Price reference for triggers — `Last`, `Mark`, `Index`, `BestBid`, `BestAsk`                               |
| `TriggerDirection`            | Trigger direction — `Above` or `Below`                                                                     |
| `TriggerMode`                 | Trigger mode — `Touch` or `Cross`                                                                          |
| `CexCapability`               | Capabilities that a CEX may or may not support (e.g., `QuoteQuantityOnLimitOrders`)                        |
| `Tag`                         | An opaque string tag for attaching metadata                                                                |
| `OrderRequest<Asset, Number>` | A request to place an order (wraps `SingleOrder`)                                                          |
| `OrderResponse`               | A response from an order submission (currently wraps a `Tag`)                                              |
| `SingleOrder<Asset, Number>`  | A single order with all parameters — base asset, quote asset, activation, pricing, side, quantity, and tag |

### Generics

Many types are generic over `Asset` and `Number`:

- **`Asset`** — The type used to identify assets. Use `AssetId` for canonical asset identifiers, or a custom type for exchange-specific identifiers.
- **`Number`** — The numeric type for prices, quantities, etc. Use `rust_decimal::Decimal` or any other numeric type.

## Example

```rust
use stock_trek_types::cex::{
    asset_id::AssetId,
    side::Side,
    quantity::Quantity,
    pricing::Pricing,
    activation::Activation,
    time_in_force::TimeInForce,
    tag::Tag,
    orders::single_order::SingleOrder,
    order_request::OrderRequest,
};
use rust_decimal::Decimal;

let order = SingleOrder {
    base: AssetId::Bitcoin,
    quote: AssetId::TetherUSD,
    activation: Activation::Immediate,
    pricing: Pricing::Limit {
        price: Decimal::new(50000, 0),
        time_in_force: TimeInForce::GoodTillCancelled,
    },
    side: Side::Buy,
    quantity: Quantity::OfBase(Decimal::new(1, 0)),
    tag: Tag::new("my-order"),
};

let request = OrderRequest::Single(order);
```

## License

MIT: See [LICENSE](LICENSE).
