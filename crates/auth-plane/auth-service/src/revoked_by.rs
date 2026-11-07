use strum::{Display, EnumString};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum RevokedBy {
    Owner,
    Admin,
    SecurityTeam,
    System,
}
