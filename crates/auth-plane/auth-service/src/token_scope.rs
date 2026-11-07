use strum::{Display, EnumString};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString)]
pub enum TokenScope {
    #[strum(to_string = "*")]
    All,
    #[strum(to_string = "orders:read")]
    OrdersRead,
    #[strum(to_string = "orders:write")]
    OrdersWrite,
    #[strum(to_string = "orders:*")]
    OrdersAll,
    #[strum(to_string = "tokens:read")]
    TokensRead,
    #[strum(to_string = "tokens:write")]
    TokensWrite,
    #[strum(to_string = "tokens:*")]
    TokensAll,
    #[strum(to_string = "admin")]
    Admin,
}

impl TokenScope {
    pub const DEFAULT: &'static [Self] = &[
        Self::OrdersRead,
        Self::OrdersWrite,
        Self::TokensRead,
        Self::TokensWrite,
        Self::Admin,
    ];

    pub fn matches(self, required: Self) -> bool {
        self == Self::All
            || self == required
            || matches!(
                (self, required),
                (Self::OrdersAll, Self::OrdersRead | Self::OrdersWrite)
                    | (Self::TokensAll, Self::TokensRead | Self::TokensWrite)
            )
    }
}
