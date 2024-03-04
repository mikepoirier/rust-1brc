use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    str::FromStr,
    sync::{mpsc::channel, Arc, Mutex},
    time::Instant,
};

use anyhow::anyhow;
use byte_unit::Byte;
use clap::Parser;
use rand::{rngs::ThreadRng, Rng};
use rust_1brc::{Station, MEASUREMENTS_FILE, STATIONS_FILE};

#[cfg(feature = "dhat-on")]
use dhat;
#[cfg(feature = "dhat-on")]
#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

fn main() -> anyhow::Result<()> {
    #[cfg(feature = "dhat-on")]
    let _dhat = dhat::Profiler::new_heap();

    let start = Instant::now();

    let cli = Cli::parse();

    let stations = StationList::read(STATIONS_FILE)?;
    let count = cli.count;
    let generator = cli.generator;

    generate(generator, count, stations, MEASUREMENTS_FILE)?;

    let elapsed = Instant::now() - start;

    println!("Run Time: {:?}", elapsed);

    Ok(())
}

fn generate(
    g: impl Generator,
    count: usize,
    stations: StationList,
    file_path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    let name = g.name();
    println!("Generator: {name}");
    g.generate(count, stations, file_path)
}

trait Generator {
    fn name(&self) -> &'static str;
    fn generate(
        self,
        count: usize,
        stations: StationList,
        file_path: impl AsRef<Path>,
    ) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
enum Generators {
    Serial(Serial),
    Parallel1(Parallel1),
    Parallel2(Parallel2),
    Parallel3(Parallel3),
}

impl FromStr for Generators {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "serial" | "s" => Ok(Self::Serial(Serial)),
            "parallel-1" | "p1" => Ok(Self::Parallel1(Parallel1::new(6))),
            "parallel-2" | "p2" => Ok(Self::Parallel2(Parallel2::new(6))),
            "parallel-3" | "p3" => Ok(Self::Parallel3(Parallel3::new(6))),
            _ => Err(anyhow!("Unsupported generator: {s}")),
        }
    }
}

impl Generator for Generators {
    fn name(&self) -> &'static str {
        match self {
            Generators::Serial(g) => g.name(),
            Generators::Parallel1(g) => g.name(),
            Generators::Parallel2(g) => g.name(),
            Generators::Parallel3(g) => g.name(),
        }
    }

    fn generate(
        self,
        count: usize,
        stations: StationList,
        file_path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        match self {
            Generators::Serial(g) => g.generate(count, stations, file_path),
            Generators::Parallel1(g) => g.generate(count, stations, file_path),
            Generators::Parallel2(g) => g.generate(count, stations, file_path),
            Generators::Parallel3(g) => g.generate(count, stations, file_path),
        }
    }
}

// ??s
#[derive(Debug, Clone)]
struct Serial;

impl Generator for Serial {
    fn name(&self) -> &'static str {
        "Serial"
    }

    fn generate(
        self,
        count: usize,
        stations: StationList,
        file_path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        let mut rng = rand::thread_rng();
        let mut data =
            Vec::with_capacity(Byte::parse_str("13 GiB", false).unwrap().as_u64() as usize);
        for _ in 0..count {
            let station = stations.rand_station(&mut rng).expect("rand station");
            let measurement = station.measurement(&mut rng).expect("measurement");
            data.extend(station.city.as_bytes().to_vec());
            data.push(b';');
            data.extend(measurement.to_string().as_bytes().to_vec());
            data.push(b'\n');
        }
        let mut file = BufWriter::new(
            File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(file_path)
                .unwrap(),
        );
        file.write_all(&data)?;
        file.flush()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Parallel1 {
    workers: usize,
}

// ??s
impl Parallel1 {
    fn new(workers: usize) -> Self {
        Self { workers }
    }
}

impl Generator for Parallel1 {
    fn name(&self) -> &'static str {
        "Parallel1"
    }

    fn generate(
        self,
        count: usize,
        stations: StationList,
        file_path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        let mut senders = vec![];
        let (tx, rx) = channel();
        let thread_count = self.workers;
        let stations = Arc::new(stations);
        for _ in 0..thread_count {
            let (worker_tx, worker_rx) = channel();
            senders.push(worker_tx);
            let tx = tx.clone();
            let stations = stations.clone();
            std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let mut data = Vec::with_capacity(
                    Byte::parse_str("13 GiB", false).unwrap().as_u64() as usize / thread_count,
                );
                for _ in worker_rx {
                    let station = stations.rand_station(&mut rng).expect("no random station");
                    let measurement = station.measurement(&mut rng)?.to_string().into_bytes();
                    data.extend(station.city.bytes());
                    data.push(b';');
                    data.extend(measurement);
                    data.push(b'\n');
                }
                tx.send(data)?;
                anyhow::Ok(())
            });
        }
        drop(tx);

        std::thread::spawn(move || {
            for n in 0..count {
                let sender = &senders[n % thread_count];
                sender.send(n)?;
            }
            anyhow::Ok(())
        });

        let mut file = BufWriter::new(
            File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(file_path)?,
        );

        for item in rx {
            file.write_all(&item)?;
        }
        file.flush()?;
        Ok(())
    }
}

// 111s
#[derive(Debug, Clone)]
struct Parallel2 {
    workers: usize,
}

impl Parallel2 {
    fn new(workers: usize) -> Self {
        Self { workers }
    }
}

impl Generator for Parallel2 {
    fn name(&self) -> &'static str {
        "Parallel2"
    }

    fn generate(
        self,
        count: usize,
        stations: StationList,
        file_path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        let mut senders = vec![];
        let thread_count = self.workers;
        let stations = Arc::new(stations);
        let file = BufWriter::new(
            File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(file_path)?,
        );
        let file = Arc::new(Mutex::new(file));
        let mut workers = vec![];
        for _ in 0..thread_count {
            let (worker_tx, worker_rx) = channel();
            senders.push(worker_tx);
            let stations = stations.clone();
            let file = file.clone();
            let worker = std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let mut buffer = Vec::with_capacity(
                    Byte::parse_str("13 GiB", false).unwrap().as_u64() as usize / thread_count,
                );
                for _ in worker_rx {
                    let station = stations.rand_station(&mut rng).expect("no random station");
                    let measurement = station.measurement(&mut rng)?.to_string().into_bytes();
                    buffer.extend(station.city.bytes());
                    buffer.push(b';');
                    buffer.extend(measurement);
                    buffer.push(b'\n');
                    if let Ok(mut file) = file.try_lock() {
                        file.write_all(&buffer)?;
                        buffer.clear();
                    }
                }
                anyhow::Ok(())
            });
            workers.push(worker);
        }

        std::thread::spawn(move || {
            for n in 0..count {
                let sender = &senders[n % thread_count];
                sender.send(n)?;
            }
            anyhow::Ok(())
        });

        for worker in workers {
            worker.join().expect("worker thread join")?;
        }

        if let Ok(mut file) = file.lock() {
            file.flush()?
        }

        Ok(())
    }
}

// 90s
#[derive(Debug, Clone)]
struct Parallel3 {
    workers: usize,
}

impl Parallel3 {
    // 64 KiB
    const MIN_BUFFER: usize = 65536;
    fn new(workers: usize) -> Self {
        Self { workers }
    }
}

impl Generator for Parallel3 {
    fn name(&self) -> &'static str {
        "Parallel3"
    }

    fn generate(
        self,
        count: usize,
        stations: StationList,
        file_path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        let mut senders = vec![];
        let thread_count = self.workers;
        let stations = Arc::new(stations);
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;
        let file = BufWriter::new(file);
        let file = Arc::new(Mutex::new(file));
        let mut workers = vec![];
        for _ in 0..thread_count {
            let (worker_tx, worker_rx) = channel();
            senders.push(worker_tx);
            let stations = stations.clone();
            let file = file.clone();
            let worker = std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let mut buffer = Vec::with_capacity(
                    Byte::parse_str("13 GiB", false).unwrap().as_u64() as usize / thread_count,
                );
                for _ in worker_rx {
                    let station = stations.rand_station(&mut rng).expect("no random station");
                    let measurement = format!("{:.1}", station.measurement(&mut rng)?);
                    buffer.extend(station.city.as_bytes());
                    buffer.push(b';');
                    buffer.extend(measurement.as_bytes());
                    buffer.push(b'\n');
                    if buffer.len() >= Self::MIN_BUFFER {
                        if let Ok(mut file) = file.try_lock() {
                            file.write_all(&buffer)?;
                            buffer.clear();
                        }
                    }
                }
                if !buffer.is_empty() {
                    if let Ok(mut file) = file.lock() {
                        file.write_all(&buffer)?;
                    }
                }
                anyhow::Ok(())
            });
            workers.push(worker);
        }

        std::thread::spawn(move || {
            for n in 0..count {
                let sender = &senders[n % thread_count];
                sender.send(n)?;
            }
            anyhow::Ok(())
        });

        for worker in workers {
            worker.join().expect("worker thread join")?;
        }

        if let Ok(mut file) = file.lock() {
            file.flush()?
        }

        Ok(())
    }
}

struct StationList {
    stations: Vec<Station>,
}

impl StationList {
    fn read(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let mut reader = csv::Reader::from_path(path)?;
        let iter = reader.deserialize();
        let mut stations = vec![];
        for record in iter {
            let record: Station = record?;
            stations.push(record);
        }
        Ok(Self { stations })
    }

    fn rand_station(&self, rng: &mut ThreadRng) -> Option<&Station> {
        let len = self.stations.len();
        let index = rng.gen_range(0..len);
        self.stations.get(index)
    }
}

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[arg(short, long)]
    count: usize,
    #[arg(short, long)]
    generator: Generators,
}
