use byte_unit::Byte;
use clap::Parser;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let bytes = cli.value.as_u64();

    println!("Bytes: {bytes}");
    Ok(())
}

#[derive(Debug, Parser)]
struct Cli {
    value: Byte,
}
