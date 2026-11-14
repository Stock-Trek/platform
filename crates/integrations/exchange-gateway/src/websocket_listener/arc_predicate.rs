use std::sync::Arc;

pub type ArcPredicate<T> = Arc<dyn for<'a> Fn(&'a T) -> bool + Send + Sync>;
