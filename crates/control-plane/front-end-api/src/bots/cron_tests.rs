use super::cron::CronExpression;
use crate::bots::bot_types::Schedule;

#[test]
fn interval_schedules_count_their_daily_runs() {
    for (every_seconds, expected) in [(10, 8640), (60, 1440), (86_400, 1), (100_000, 1)] {
        let schedule = Schedule::Interval {
            every_seconds,
            misfire_grace_seconds: None,
        }
        .validated()
        .expect("interval");
        assert_eq!(schedule.runs_per_day(), expected);
    }
}

#[test]
fn cron_schedules_are_expanded_to_count_daily_runs() {
    for (expression, expected) in [
        ("* * * * *", 1440),
        ("*/10 * * * *", 144),
        ("0 * * * *", 24),
        ("0 0 * * *", 1),
        ("0 0 * * 0", 1),
        ("0 0 1 * *", 1),
        ("0 0 1 1 *", 1),
        ("0,30 * * * *", 48),
        ("0 0,12 * * *", 2),
        ("0 9-17 * * 1-5", 7),
        ("*/5 8-16 * * 1-5", 77),
    ] {
        let schedule = Schedule::Cron {
            expression: expression.to_string(),
            timezone: "UTC".to_string(),
            misfire_grace_seconds: None,
        }
        .validated()
        .expect("cron");
        assert_eq!(
            schedule.runs_per_day(),
            expected,
            "unexpected expansion for {expression}"
        );
    }
}

#[test]
fn cron_validation_rejects_malformed_expressions() {
    for expression in [
        "* * * *",
        "* * * * * *",
        "60 * * * *",
        "* 24 * * *",
        "* * 0 * *",
        "* * * 0 *",
        "* * * * 8",
        "5-1 * * * *",
        "a * * * *",
        "*/0 * * * *",
    ] {
        assert!(
            CronExpression::validated(expression).is_err(),
            "{expression} must be rejected"
        );
    }
}

#[test]
fn unknown_cron_dialects_count_nothing() {
    assert_eq!(CronExpression::runs_per_day("not a cron"), 0);
    assert_eq!(CronExpression::runs_per_day(""), 0);
}
