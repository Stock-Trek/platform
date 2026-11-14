use platform_errors::PlatformResult;
use std::{future::Future, pin::Pin};

pub type SubmissionFuture<'connector, Request, Response, VerificationRequest> = Pin<
    Box<
        dyn Future<
                Output = PlatformResult<SubmissionOutcome<Request, Response, VerificationRequest>>,
            > + Send
            + 'connector,
    >,
>;

#[derive(Debug)]
pub enum SubmissionOutcome<Request, Response, VerificationRequest> {
    Submitted(Response),
    Unknown {
        retry: Option<Request>,
        verify: Option<VerificationRequest>,
    },
}
