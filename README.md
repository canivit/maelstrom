# Maelstrom

This repo contains my Rust solutions to Fly.io's series of
[distributed systems challenges](https://fly.io/dist-sys/).

## Environment Setup

1.  Clone the repo and `cd` into it.
    A `flake.nix` is provided, which can create a reproducible environment for running `maelstrom`
    test harness with the compiled Rust binaries.

        [More about Nix](https://nixos.org/).

2.  Run `nix flake` in the root of the repo to setup the environment.
    This will install Rust dependencies (`cargo`, `rustfmt`, `clippy`) and the maelstrom test harness.
    The test harness will be available to run with the command `maelstrom`.

3.  Build binaries for all challenges:
    ```bash
    cargo build -r
    ```
4.  Run the test harness for any challenge. For example, challenge [5b](https://fly.io/dist-sys/5b/)
    can be run with the following:
    ```bash
    maelstrom test -w kafka --bin ./target/release/kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
    ```

If you don't want to use Nix, Rust dependencies and maelstrom test harness can be installed
[manually](https://github.com/jepsen-io/maelstrom) as well.

## Challenges

| Challenge                                                                                               | Binary                                                                              |
| ------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [1](https://fly.io/dist-sys/1/)                                                                         | [echo.rs](https://github.com/canivit/maelstrom/blob/main/src/bin/echo.rs)           |
| [2](https://fly.io/dist-sys/2/)                                                                         | [unique.rs](https://github.com/canivit/maelstrom/blob/main/src/bin/unique.rs)       |
| [3a](https://fly.io/dist-sys/3a/), [3b](https://fly.io/dist-sys/3b/), [3c](https://fly.io/dist-sys/3c/) | [broadcast.rs](https://github.com/canivit/maelstrom/blob/main/src/bin/broadcast.rs) |
| [4](https://fly.io/dist-sys/4/)                                                                         | [gcounter.rs](https://github.com/canivit/maelstrom/blob/main/src/bin/gcounter.rs)   |
| [5a](https://fly.io/dist-sys/5a/), [5b](https://fly.io/dist-sys/5b/)                                    | [kafka.rs](https://github.com/canivit/maelstrom/blob/main/src/bin/kafka.rs)         |
| [6a](https://fly.io/dist-sys/6a/), [6b](https://fly.io/dist-sys/6b/), [6c](https://fly.io/dist-sys/6c/) | [txn.rs](https://github.com/canivit/maelstrom/blob/main/src/bin/txn.rs)             |
