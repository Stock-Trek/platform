use serde::{Deserialize, Serialize};

use super::{
    arithmetic::{ArithmeticError, CheckedDiv, CheckedRem, RawCheckedRem},
    arithmetic_impls::arithmetic_impls,
    new_type::new_type,
    non_zero::NonZero,
};

new_type!(UsageCount, u64);
arithmetic_impls!(UsageCount, u64);
