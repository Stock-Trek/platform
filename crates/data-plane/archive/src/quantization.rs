use crate::{top_of_hour::TopOfHour, top_of_minute::TopOfMinute, top_of_month::TopOfMonth};
use chrono::Weekday;

#[derive(Debug, Clone, Copy)]
pub enum Quantization {
    Minutes(TopOfMinute),
    Hour(TopOfHour),
    Day,
    Week(Weekday),
    Month(TopOfMonth),
    Year,
}
