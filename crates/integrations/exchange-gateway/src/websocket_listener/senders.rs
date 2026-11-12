use parking_lot::Mutex;
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::mpsc::UnboundedSender;

pub(crate) type Senders = Arc<Mutex<BTreeMap<u64, UnboundedSender<Value>>>>;
