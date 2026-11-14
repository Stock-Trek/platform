use crate::rebuild::spill_codec::{SpillCodec, SpillReader};
use platform_errors::PlatformResult;
use std::{cmp::Ordering, collections::BinaryHeap, vec::IntoIter};

pub enum SpillRecords<C: SpillCodec> {
    Memory(IntoIter<C::Record>),
    Runs(SpillMerge<C>),
}

impl<C: SpillCodec> SpillRecords<C> {
    pub fn next_record(&mut self) -> PlatformResult<Option<C::Record>> {
        match self {
            Self::Memory(records) => Ok(records.next()),
            Self::Runs(merge) => merge.next_record(),
        }
    }
}

struct SpillEntry<K, R> {
    key: K,
    run: usize,
    precedence: usize,
    record: R,
}

impl<K: Ord, R> PartialEq for SpillEntry<K, R> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<K: Ord, R> Eq for SpillEntry<K, R> {}

impl<K: Ord, R> PartialOrd for SpillEntry<K, R> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord, R> Ord for SpillEntry<K, R> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| self.precedence.cmp(&other.precedence))
    }
}

pub struct SpillMerge<C: SpillCodec> {
    runs: Vec<C::Reader>,
    heap: BinaryHeap<SpillEntry<C::Key, C::Record>>,
    codec: C,
}

impl<C: SpillCodec> SpillMerge<C> {
    pub fn new(mut runs: Vec<C::Reader>, codec: C) -> PlatformResult<Self> {
        let mut heap = BinaryHeap::with_capacity(runs.len());
        let prefer_latest = codec.prefer_latest_run();
        let precedence = |run: usize| {
            if prefer_latest { run } else { usize::MAX - run }
        };
        for (run, reader) in runs.iter_mut().enumerate() {
            if let Some(record) = reader.read()? {
                heap.push(SpillEntry {
                    key: codec.key(&record),
                    run,
                    precedence: precedence(run),
                    record,
                });
            }
        }
        Ok(Self { runs, heap, codec })
    }

    pub fn next_record(&mut self) -> PlatformResult<Option<C::Record>> {
        let Some(best) = self.heap.pop() else {
            return Ok(None);
        };
        self.advance(best.run)?;
        if self.codec.dedupe_merges() {
            while self
                .heap
                .peek()
                .is_some_and(|candidate| candidate.key == best.key)
            {
                let Some(candidate) = self.heap.pop() else {
                    break;
                };
                self.advance(candidate.run)?;
            }
        }
        Ok(Some(best.record))
    }

    fn advance(&mut self, run: usize) -> PlatformResult<()> {
        if let Some(record) = self.runs[run].read()? {
            let precedence = if self.codec.prefer_latest_run() {
                run
            } else {
                usize::MAX - run
            };
            self.heap.push(SpillEntry {
                key: self.codec.key(&record),
                run,
                precedence,
                record,
            });
        }
        Ok(())
    }
}
