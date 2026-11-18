use super::ws::PongTracker;

#[test]
fn a_connection_closes_after_two_missed_pongs() {
    let mut pongs = PongTracker::new();
    assert!(!pongs.on_ping());
    assert!(!pongs.on_ping());
    assert!(pongs.on_ping());
}

#[test]
fn a_pong_resets_the_missed_count() {
    let mut pongs = PongTracker::new();
    assert!(!pongs.on_ping());
    pongs.on_pong();
    assert!(!pongs.on_ping());
    assert!(!pongs.on_ping());
    assert!(pongs.on_ping());
}
