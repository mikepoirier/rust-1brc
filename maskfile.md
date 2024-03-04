# Rust 1BRC Tasks

## main

```sh
$MASK build && ./target/release/rust-1brc
```

### hyperfine

```sh
hyperfine --warmup 1 \
    "target/release/rust-1brc"
```

### perf-stat

```sh
perf stat -ad -r 10 target/release/rust-1brc
```

### perf-record

```sh
perf record -e L1-dcache-loads,LLC-load-misses --call-graph dwarf -- target/release/rust-1brc
```

### flamegraph

```sh
$MASK build && \
$MASK main perf-record && \
perf script | inferno-collapse-perf > stacks.folded && \
bat stacks.folded | inferno-flamegraph > profile.svg && \
firefox profile.svg
```

### dhat

```sh
cargo run --bin rust-1brc --features dhat-on
```

## generate (n)

```sh
$MASK build && ./target/release/generator -c $n -g p3
```

### hyperfine

```sh
hyperfine --warmup 1 \
    "target/release/generator -c 10000000 -g s" \
    "target/release/generator -c 10000000 -g p1" \
    "target/release/generator -c 10000000 -g p2" \
    "target/release/generator -c 10000000 -g p3"
```

### perf-stat

```sh
perf stat -ad -r 10 target/release/generator -c 10000000 -g p3
```

### perf-record

```sh
perf record -e L1-dcache-loads,LLC-load-misses --call-graph dwarf -- target/release/generator -c 10000000 -g p3
```

### flamegraph

```sh
$MASK build && \
$MASK perf-record && \
perf script | inferno-collapse-perf > stacks.folded && \
bat stacks.folded | inferno-flamegraph > profile.svg && \
firefox profile.svg
```

### dhat

```sh
cargo run --bin generator --features dhat-on -- -c 10000000 -g p3
```

## build

```sh
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

