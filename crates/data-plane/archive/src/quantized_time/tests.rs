use super::*;
use chrono::{DateTime, TimeZone, Utc, Weekday};

fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
        .single()
        .unwrap()
}

#[test]
fn top_at_or_after_moves_to_the_next_quantized_top() {
    let six_minutes = Quantization::Minutes(TopOfMinute::Six);
    assert_eq!(
        QuantizedTime::top_at_or_after(utc(2024, 1, 1, 0, 1), six_minutes).unwrap(),
        utc(2024, 1, 1, 0, 6)
    );
    assert_eq!(
        QuantizedTime::top_at_or_after(utc(2024, 1, 1, 0, 6), six_minutes).unwrap(),
        utc(2024, 1, 1, 0, 6)
    );

    let six_hours = Quantization::Hour(TopOfHour::Six);
    assert_eq!(
        QuantizedTime::top_at_or_after(utc(2024, 1, 1, 1, 0), six_hours).unwrap(),
        utc(2024, 1, 1, 6, 0)
    );

    let monday = Quantization::Week(Weekday::Mon);
    assert_eq!(
        QuantizedTime::top_at_or_after(utc(2024, 1, 3, 12, 0), monday).unwrap(),
        utc(2024, 1, 8, 0, 0)
    );

    let six_months = Quantization::Month(TopOfMonth::Six);
    assert_eq!(
        QuantizedTime::top_at_or_after(utc(2024, 2, 15, 0, 0), six_months).unwrap(),
        utc(2024, 7, 1, 0, 0)
    );
}
