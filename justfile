default:
    just --list

fmt:
    cargo fmt

pub:
    cargo publish

check-all:
    cargo check --no-default-features --features arrow
    cargo check --no-default-features --features geoarrow-08
    cargo check --no-default-features --features polars
    cargo check --no-default-features --features polars-51
    cargo check --no-default-features --features polars-53
