use super::arithmetic::RawCheckedRem;
use super::new_type::new_type;
use serde::{Deserialize, Serialize};

new_type!(DurationSeconds, i64);
new_type!(DurationMilliseconds, i64);
new_type!(DurationMicroseconds, i64);
new_type!(DurationNanoseconds, i64);
