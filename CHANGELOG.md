## 58.0.0

### Breaking changes

- `FromArrowRobj`, `ToArrowRobj`, and `IntoArrowRobj` traits moved from `arrow_extendr::from` / `arrow_extendr::to` to the crate root (`arrow_extendr`). Update imports accordingly.
- `FromArrowRobj::from_arrow_robj` now returns `std::result::Result<Self, anyhow::Error>` instead of `Result<Self, ArrowError>`. This provides a uniform error type across both the `arrow` and `polars` feature implementations.
- `arrow-rs` is now an optional dependency enabled via the `arrow` feature flag (on by default). Users who previously depended on arrow types being always available should add `features = ["arrow"]` to their dependency or rely on the default.
- `ErrArrowRobj` type alias removed. Use `anyhow::Error` directly.

### New features

- Add `polars` feature flag enabling interop with `polars-core` (which re-exports `polars-arrow`)
- Implements `FromArrowRobj` for `polars_arrow::ffi::ArrowArrayStreamReader` — import a `nanoarrow_array_stream` from R into a polars-arrow stream reader
- Implements `IntoArrowRobj` for `polars_arrow::ffi::ArrowArrayStream` — export any polars-arrow stream to R as a `nanoarrow_array_stream`
- Implements `IntoArrowRobj` for `polars_core::frame::DataFrame` — export a Polars `DataFrame` to R as a `nanoarrow_array_stream`, preserving chunking
- Implements `FromArrowRobj` for `polars_core::frame::DataFrame` — import a `nanoarrow_array_stream` from R into a Polars `DataFrame`

## 52.0.0

- Release compatible with arrow-rs 52.0.0

## 51.0.0

- Release compatible with arrow-rs 51.0.0

## 50.0.0

- Release compatible with arrow-rs 50.0.0

## 49.0.0-geoarrow (2027-11-28)

- Release compatible with geoarrow-rs based on [the Cargo.toml](https://github.com/geoarrow/geoarrow-rs/blob/9a0df7fad02fd5d4c84a23fe3ebf1a5d05e71d1e/Cargo.toml)

## 49.0.0 (2027-11-27)

- Release compatible with arrow-rs 49.0.0

## 48.0.1 (2027-11-27)

- Initial Crates.io release compatible with arrow-rs 48.0.1
