pub mod baseline;
pub mod rayon_mmap_string;
pub mod rayon_string;

pub trait Processor {
    fn process(&self, file: &str) -> anyhow::Result<()>;
}

pub fn process(processor: impl Processor, file: &str) -> anyhow::Result<()> {
    processor.process(file)?;
    Ok(())
}
