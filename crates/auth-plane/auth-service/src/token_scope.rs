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
    /// Allows reading token metadata through a redacted [`crate::token_view::TokenView`].
    #[strum(to_string = "tokens:read")]
    TokensRead,
    /// Allows administering tokens.
    ///
    /// This is an admin-level scope: it can mint tokens with arbitrary scopes,
    /// including [`TokenScope::Admin`], and rotate or revoke any token.
    #[strum(to_string = "tokens:write")]
    TokensWrite,
    #[strum(to_string = "tokens:*")]
    TokensAll,
    #[strum(to_string = "admin")]
    Admin,
}

impl TokenScope {
    pub const DEFAULT: &'static [Self] = &[Self::OrdersRead, Self::OrdersWrite];

    pub fn matches(self, required: Self) -> bool {
        self == Self::All
            || self == Self::Admin
            || self == required
            || matches!(
                (self, required),
                (Self::OrdersAll, Self::OrdersRead | Self::OrdersWrite)
                    | (Self::TokensAll, Self::TokensRead | Self::TokensWrite)
            )
    }
}
