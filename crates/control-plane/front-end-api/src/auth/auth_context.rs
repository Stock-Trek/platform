use crate::api::Problem;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Scope {
    Read,
    Manage,
    Admin,
}

#[derive(Debug, Clone)]
pub(crate) struct AuthContext {
    owner: String,
    token: String,
    scopes: Vec<Scope>,
}

impl AuthContext {
    pub(crate) fn new(
        owner: impl Into<String>,
        token: impl Into<String>,
        scopes: Vec<Scope>,
    ) -> Self {
        Self {
            owner: owner.into(),
            token: token.into(),
            scopes,
        }
    }

    pub(crate) fn owner(&self) -> &str {
        &self.owner
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    pub(crate) fn require(&self, scope: Scope) -> Result<(), Problem> {
        if self.scopes.contains(&scope) {
            Ok(())
        } else {
            Err(Problem::forbidden(format!(
                "the token lacks the {} scope",
                scope.as_str()
            )))
        }
    }

    pub(crate) fn require_admin(&self) -> Result<(), Problem> {
        if self.scopes.contains(&Scope::Admin) {
            Ok(())
        } else {
            Err(Problem::step_up_required())
        }
    }
}

impl Scope {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Manage => "manage",
            Self::Admin => "admin",
        }
    }
}
