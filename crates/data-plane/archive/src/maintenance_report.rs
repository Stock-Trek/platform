use crate::MaintenanceStage;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceBlock {
    pub code: String,
    pub message: String,
    pub permanent: bool,
}

impl MaintenanceBlock {
    pub fn new(code: impl Into<String>, message: impl Into<String>, permanent: bool) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            permanent,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageOutcome {
    pub stage: MaintenanceStage,
    pub planned: usize,
    pub applied: usize,
    pub blocked: Option<MaintenanceBlock>,
}

impl StageOutcome {
    pub fn completed(stage: MaintenanceStage, planned: usize, applied: usize) -> Self {
        Self {
            stage,
            planned,
            applied,
            blocked: None,
        }
    }

    pub fn blocked(stage: MaintenanceStage, planned: usize, block: MaintenanceBlock) -> Self {
        Self {
            stage,
            planned,
            applied: 0,
            blocked: Some(block),
        }
    }

    pub fn is_blocked(&self) -> bool {
        self.blocked.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceRunReport {
    pub dry_run: bool,
    pub acquired: bool,
    pub completed: bool,
    pub stages: Vec<StageOutcome>,
}

impl MaintenanceRunReport {
    pub fn blocked(reason: MaintenanceBlock) -> Self {
        Self {
            dry_run: false,
            acquired: false,
            completed: false,
            stages: Vec::new(),
        }
        .with_block(reason)
    }

    pub fn with_block(mut self, block: MaintenanceBlock) -> Self {
        self.stages.push(StageOutcome::blocked(
            MaintenanceStage::ExpireSnapshots,
            0,
            block,
        ));
        self
    }
}
