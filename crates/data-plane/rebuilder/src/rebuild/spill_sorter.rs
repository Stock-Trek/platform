use super::{
    SpillUsage,
    spill::SpillMergeLimits,
    spill_codec::{SpillCodec, SpillWriter},
    spill_merge::{SpillMerge, SpillRecords},
};
use platform_errors::{BuildError, BuildResult};
use std::path::{Path, PathBuf};

pub(crate) struct SpillSorter<C: SpillCodec> {
    directory: PathBuf,
    limit: usize,
    codec: C,
    entries: Vec<(C::Key, C::Record)>,
    runs: Vec<PathBuf>,
    generated: usize,
    spilled: usize,
    usage: SpillUsage,
}

impl<C: SpillCodec> SpillSorter<C> {
    pub(crate) fn with_codec(
        directory: impl Into<PathBuf>,
        limit: usize,
        codec: C,
    ) -> BuildResult<Self> {
        let directory = directory.into();
        std::fs::create_dir_all(&directory).map_err(|error| {
            BuildError::io(
                "create rebuild spill directory",
                directory.display().to_string(),
                error,
            )
        })?;
        Ok(Self {
            directory,
            limit: limit.max(1),
            codec,
            entries: Vec::new(),
            runs: Vec::new(),
            generated: 0,
            spilled: 0,
            usage: SpillUsage::default(),
        })
    }

    pub(crate) fn push_record(&mut self, record: C::Record) -> BuildResult<()> {
        let key = self.codec.key(&record);
        self.entries.push((key, record));
        if self.entries.len() >= self.limit {
            self.spill()?;
        }
        Ok(())
    }

    pub(crate) fn spilled_records(&self) -> usize {
        self.spilled
    }

    pub(crate) fn usage(&self) -> SpillUsage {
        self.usage
    }

    pub(crate) fn finish(&mut self) -> BuildResult<SpillRecords<C>> {
        if self.runs.is_empty() {
            self.entries
                .sort_by(|(left, _), (right, _)| left.cmp(right));
            let records = self.codec.prepare(std::mem::take(&mut self.entries));
            return Ok(SpillRecords::Memory(records.into_iter()));
        }

        self.spill()?;
        self.reduce_runs(SpillMergeLimits::fanin(self.limit))?;
        let batch = SpillMergeLimits::batch_records(self.limit);
        let readers = self
            .runs
            .iter()
            .map(|path| self.codec.open(path, batch))
            .collect::<BuildResult<Vec<_>>>()?;
        Ok(SpillRecords::Runs(SpillMerge::new(
            readers,
            self.codec.clone(),
        )?))
    }

    fn spill(&mut self) -> BuildResult<()> {
        if self.entries.is_empty() {
            return Ok(());
        }

        self.entries
            .sort_by(|(left, _), (right, _)| left.cmp(right));
        let records = self.codec.prepare(std::mem::take(&mut self.entries));
        self.spilled += records.len();
        let path = self.next_run_path("run");
        let mut writer = self
            .codec
            .create(&path, SpillMergeLimits::batch_records(self.limit))?;
        for record in records {
            writer.write(record)?;
        }
        writer.finish()?;
        self.usage.record(&path)?;
        self.runs.push(path);
        Ok(())
    }

    fn reduce_runs(&mut self, fan_in: usize) -> BuildResult<()> {
        while self.runs.len() > fan_in {
            let sources = std::mem::take(&mut self.runs);
            let mut merged = Vec::new();
            for chunk in sources.chunks(fan_in) {
                if chunk.len() == 1 {
                    merged.push(chunk[0].clone());
                    continue;
                }
                let path = self.next_run_path("merge");
                self.merge_runs(chunk, &path)?;
                for source in chunk {
                    std::fs::remove_file(source).map_err(|error| {
                        BuildError::io(
                            "remove merged rebuild spill run",
                            source.display().to_string(),
                            error,
                        )
                    })?;
                }
                merged.push(path);
            }
            self.runs = merged;
        }
        Ok(())
    }

    fn next_run_path(&mut self, stage: &str) -> PathBuf {
        let path = self.directory.join(format!(
            "{}-{stage}-{}.{}",
            self.codec.label(),
            self.generated,
            self.codec.extension()
        ));
        self.generated += 1;
        path
    }

    fn merge_runs(&mut self, runs: &[PathBuf], path: &Path) -> BuildResult<()> {
        let batch = SpillMergeLimits::batch_records(self.limit);
        let readers = runs
            .iter()
            .map(|run| self.codec.open(run, batch))
            .collect::<BuildResult<Vec<_>>>()?;
        let mut merge = SpillMerge::new(readers, self.codec.clone())?;
        let mut writer = self.codec.create(path, batch)?;
        while let Some(record) = merge.next_record()? {
            writer.write(record)?;
        }
        writer.finish()?;
        self.usage.record(path)
    }
}
