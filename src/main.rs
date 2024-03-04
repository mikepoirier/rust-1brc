use rust_1brc::{
    processor::{
        baseline::Baseline, process, rayon_mmap_string::RayonMmapString, rayon_string::RayonString,
    },
    MEASUREMENTS_FILE,
};

fn main() -> anyhow::Result<()> {
    process(RayonString, MEASUREMENTS_FILE)?;

    Ok(())
}
