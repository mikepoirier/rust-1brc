use std::{ops::AddAssign, str::FromStr, sync::Arc};

use anyhow::anyhow;
use rand::{rngs::ThreadRng, Rng};
use serde::Deserialize;

pub mod processor;

pub static MEASUREMENTS_FILE: &str = "measurements.txt";
pub static STATIONS_FILE: &str = "stations.csv";

#[derive(Debug, Deserialize)]
pub struct Station {
    pub city: Arc<str>,
    pub temp: f64,
}

impl Station {
    pub fn measurement(&self, rng: &mut ThreadRng) -> anyhow::Result<f64> {
        let measurement = rng.sample(rand_distr::Normal::new(self.temp, 10.0)?);

        Ok((measurement * 10.0).round() / 10.0)
    }
}

impl FromStr for Station {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((city, temp)) = s.split_once(';') else {
            return Err(anyhow!("Cannot parse station: {s}"));
        };
        let city = city.into();
        let temp = temp.parse()?;

        Ok(Self { city, temp })
    }
}

struct TempResult {
    min: f64,
    max: f64,
    total: f64,
    count: usize,
}

impl TempResult {
    fn new(station: &Station) -> Self {
        Self {
            min: station.temp,
            max: station.temp,
            total: station.temp,
            count: 1,
        }
    }
}

impl AddAssign<&Station> for TempResult {
    fn add_assign(&mut self, station: &Station) {
        if self.min > station.temp {
            self.min = station.temp;
        }
        if self.max < station.temp {
            self.max = station.temp;
        }
        self.count += 1;
        self.total += station.temp;
    }
}
