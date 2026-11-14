use serde::{Deserialize, Serialize};

use super::{
    arithmetic::{ArithmeticError, CheckedDiv, CheckedRem, RawCheckedRem},
    arithmetic_impls::arithmetic_impls,
    new_type::new_type,
    non_zero::NonZero,
};

new_type!(DurationSeconds, i64);
arithmetic_impls!(DurationSeconds, i64);

new_type!(DurationMilliseconds, i64);
arithmetic_impls!(DurationMilliseconds, i64);

new_type!(DurationMicroseconds, i64);
arithmetic_impls!(DurationMicroseconds, i64);

new_type!(DurationNanoseconds, i64);
arithmetic_impls!(DurationNanoseconds, i64);
