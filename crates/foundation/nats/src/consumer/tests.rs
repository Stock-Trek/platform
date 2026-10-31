use super::*;

#[test]
fn resume_policy_starts_at_the_requested_sequence() {
    assert_eq!(
        JetStreamConsumer::deliver_policy(Some(42)),
        DeliverPolicy::ByStartSequence { start_sequence: 42 }
    );
}

#[test]
fn missing_sequence_starts_from_the_beginning() {
    assert_eq!(JetStreamConsumer::deliver_policy(None), DeliverPolicy::All);
    assert_eq!(
        JetStreamConsumer::deliver_policy(Some(0)),
        DeliverPolicy::All
    );
}

#[test]
fn explicit_start_positions_map_to_delivery_policies() {
    assert_eq!(
        JetStreamConsumer::deliver_policy_for_start(ConsumerStart::Earliest),
        DeliverPolicy::All
    );
    assert_eq!(
        JetStreamConsumer::deliver_policy_for_start(ConsumerStart::Latest),
        DeliverPolicy::New
    );
    assert_eq!(
        JetStreamConsumer::deliver_policy_for_start(ConsumerStart::Sequence(9)),
        DeliverPolicy::ByStartSequence { start_sequence: 9 }
    );
}
