use crate::RevokedBy;

struct RevokedByFixture;

impl RevokedByFixture {
    fn actors() -> [RevokedBy; 4] {
        [
            RevokedBy::Owner,
            RevokedBy::Admin,
            RevokedBy::SecurityTeam,
            RevokedBy::System,
        ]
    }
}

#[test]
fn actors_have_stable_string_representations() {
    let expected = [
        (RevokedBy::Owner, "owner"),
        (RevokedBy::Admin, "admin"),
        (RevokedBy::SecurityTeam, "security_team"),
        (RevokedBy::System, "system"),
    ];

    for (actor, value) in expected {
        assert_eq!(actor.to_string(), value);
    }
}

#[test]
fn actors_round_trip_through_their_string_representation() {
    for actor in RevokedByFixture::actors() {
        assert_eq!(actor.to_string().parse::<RevokedBy>().unwrap(), actor);
    }
}
