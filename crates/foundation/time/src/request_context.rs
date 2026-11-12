use crate::deadline_budget::Deadline;

#[derive(Debug, Clone)]
pub struct RequestContext<Context> {
    pub context: Context,
    pub deadline: Deadline,
}
