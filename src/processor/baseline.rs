use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead as _, BufReader},
    sync::Arc,
};

use crate::{Station, TempResult};

use super::Processor;

// 211s
pub struct Baseline;

impl Processor for Baseline {
    fn process(&self, file: &str) -> anyhow::Result<()> {
        let file = File::options().read(true).open(file)?;
        let file = BufReader::new(file);
        let mut results: HashMap<Arc<str>, TempResult> = HashMap::new();

        for line in file.lines() {
            let line = line?;
            let station = line.parse::<Station>()?;

            results
                .entry(station.city.clone())
                .and_modify(|result| *result += &station)
                .or_insert(TempResult::new(&station));
        }

        let mut output = String::new();
        output.push('{');
        for (key, result) in results {
            output.push_str(&key);
            output.push('=');
            let min = result.min;
            let mean = result.total / result.count as f64;
            let max = result.max;
            output.push_str(&format!("{min:.1}/{mean:.1}/{max:.1}"));
        }
        println!("{output}");

        Ok(())
    }
}
