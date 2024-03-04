use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::Read,
    ops::{Add, Deref, DerefMut},
    sync::Arc,
};

use rayon::{iter::ParallelIterator, str::ParallelString};

use crate::{Station, TempResult};

use super::Processor;

pub struct RayonString;

impl Processor for RayonString {
    fn process(&self, file: &str) -> anyhow::Result<()> {
        let mut file = File::options().read(true).open(file)?;
        let file_size = file.metadata().expect("file metadata").len();
        let mut data = String::with_capacity(file_size as usize);
        file.read_to_string(&mut data)?;
        let results = data
            .par_lines()
            .map(|line| line.parse::<Station>().expect("parse station"))
            .fold(
                || Results::default(),
                |mut results, station| {
                    results
                        .entry(station.city.clone())
                        .and_modify(|result| *result += &station)
                        .or_insert(TempResult::new(&station));
                    results
                },
            )
            .reduce(|| Results::default(), |acc, next| acc + next);

        let mut output = String::new();
        for (key, result) in results.into_inner() {
            if !output.is_empty() {
                output.push_str(", ");
            }
            output.push_str(&key);
            output.push('=');
            let min = result.min;
            let mean = result.total / result.count as f64;
            let max = result.max;
            output.push_str(&format!("{min:.1}/{mean:.1}/{max:.1}"));
        }
        println!("{{{output}}}");

        Ok(())
    }
}

#[derive(Default)]
struct Results(BTreeMap<Arc<str>, TempResult>);

impl Results {
    fn into_inner(self) -> BTreeMap<Arc<str>, TempResult> {
        self.0
    }
}

impl Deref for Results {
    type Target = BTreeMap<Arc<str>, TempResult>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Results {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Add for Results {
    type Output = Results;

    fn add(self, rhs: Self) -> Self::Output {
        let mut data = self.0;
        data.extend(rhs.0);
        Self(data)
    }
}
