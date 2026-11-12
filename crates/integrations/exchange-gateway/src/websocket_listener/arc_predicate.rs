use std::sync::Arc;

pub(crate) type ArcPredicate<T> = Arc<dyn for<'a> Fn(&'a T) -> bool + Send + Sync>;
