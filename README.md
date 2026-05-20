# arrow-extendr

arrow-extendr is a crate that facilitates the transfer of [Apache Arrow](https://arrow.apache.org/) memory between R and Rust. It utilizes [extendr](https://extendr.github.io/) and [arrow-rs](https://docs.rs/arrow).

Arrow memory is exchanged via the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) using R's `nanoarrow_array`, `nanoarrow_schema`, and `nanoarrow_array_stream` external pointer objects. The [`{nanoarrow}`](https://arrow.apache.org/nanoarrow/0.3.0/r/index.html) R package is **not** required as a dependency of your package.

## Features

| Feature | Description |
| ------- | ----------- |
| `arrow` | Core arrow-rs interop (default) |
| `geoarrow-08` | GeoArrow array support via geoarrow-array 0.8 |
| `polars` | Polars interop, alias for `polars-53` |
| `polars-53` | Polars interop for polars-core 0.53 |
| `polars-51` | Polars interop for polars-core 0.51 |

## Motivating Example

Say we have the following `DBI` connection which we will send requests to using arrow.
The result of `dbGetQueryArrow()` is a `nanoarrow_array_stream`. We want to
count the number of rows in each batch of the stream using Rust.

```r
# adapted from https://github.com/r-dbi/DBI/blob/main/vignettes/DBI-arrow.Rmd

library(DBI)
con <- dbConnect(RSQLite::SQLite())
data <- data.frame(
  a = runif(10000, 0, 10),
  b = rnorm(10000, 4.5),
  c = sample(letters, 10000, TRUE)
)

dbWriteTable(con, "tbl", data)
```

We can write an extendr function which creates an `ArrowArrayStreamReader`
from an `&Robj`. In the function we instantiate a counter to keep track
of the number of rows per chunk. For each chunk we print the number of rows.

```rust
use extendr_api::prelude::*;
use arrow_extendr::FromArrowRobj;
use arrow::ffi_stream::ArrowArrayStreamReader;

#[extendr]
/// @export
fn process_stream(stream: Robj) -> i32 {
    let rb = ArrowArrayStreamReader::from_arrow_robj(&stream)
        .unwrap();

    let mut n = 0;

    rprintln!("Processing `ArrowArrayStreamReader`...");
    for chunk in rb {
        let chunk_rows = chunk.unwrap().num_rows();
        rprintln!("Found {chunk_rows} rows");
        n += chunk_rows as i32;
    }

    n
}
```

With this function we can use it on the output of `dbGetQueryArrow()` or other Arrow
related DBI functions.

```r
query <- dbGetQueryArrow(con, "SELECT * FROM tbl WHERE a < 3")
process_stream(query)
#> Processing `ArrowArrayStreamReader`...
#> Found 256 rows
#> Found 256 rows
#> Found 256 rows
#> ... truncated ...
#> Found 256 rows
#> Found 256 rows
#> Found 143 rows
#> [1] 2959
```

## GeoArrow interop

arrow-extendr supports [GeoArrow](https://geoarrow.org/) via the `geoarrow-08` feature flag, backed by the [geoarrow-array](https://docs.rs/geoarrow-array) crate.

```toml
arrow_extendr = { version = "58.0.1", features = ["geoarrow-08"] }
geoarrow-array = "0.8"
```

This enables the following conversions between Rust and R's `nanoarrow_array` objects:

| Type | `FromArrowRobj` | `ToArrowRobj` | `IntoArrowRobj` |
| ---- | :-------------: | :-----------: | :-------------: |
| `Arc<dyn GeoArrowArray>` | ✓ | ✓ | ✓ |
| `PointArray` | ✓ | ✓ | ✓ |
| `LineStringArray` | ✓ | ✓ | ✓ |
| `PolygonArray` | ✓ | ✓ | ✓ |
| `MultiPointArray` | ✓ | ✓ | ✓ |
| `MultiLineStringArray` | ✓ | ✓ | ✓ |
| `MultiPolygonArray` | ✓ | ✓ | ✓ |
| `GeometryArray` | ✓ | ✓ | ✓ |
| `GeometryCollectionArray` | ✓ | ✓ | ✓ |
| `RectArray` | ✓ | ✓ | ✓ |
| `WkbViewArray` | ✓ | ✓ | ✓ |
| `WktViewArray` | ✓ | ✓ | ✓ |

### Example: round-trip a GeoArrow array through R

```rust
use extendr_api::prelude::*;
use arrow_extendr::{FromArrowRobj, IntoArrowRobj};
use geoarrow_array::array::PointArray;

#[extendr]
/// @export
fn geoarrow_round_trip(x: Robj) -> extendr_api::Result<Robj> {
    let array = PointArray::from_arrow_robj(&x)
        .map_err(|e| extendr_api::Error::Other(e.to_string()))?;
    array.into_arrow_robj()
}
```

```r
library(geoarrow)
library(wk)

pts <- as_geoarrow_array(xy(c(1, 2, 3), c(4, 5, 6)))
geoarrow_round_trip(pts)
```

## Polars interop

arrow-extendr provides optional interop with [Polars](https://docs.rs/polars) via versioned feature flags. Use the feature that matches your `polars-core` version:

| Feature | polars-core version |
| ------- | ------------------- |
| `polars` | `0.53` (alias for `polars-53`) |
| `polars-53` | `0.53` |
| `polars-51` | `0.51` |

These features are mutually exclusive — enabling more than one will produce a compile error.

### polars-core 0.53

```toml
arrow_extendr = { version = "58.0.1", features = ["polars-53"], default-features = false }
polars-core = "0.53.0"
anyhow = "1"
```

### polars-core 0.51

```toml
arrow_extendr = { version = "58.0.1", features = ["polars-51"], default-features = false }
polars-core = "0.51.0"
anyhow = "1"
```

This enables the following conversions via the Arrow C Stream interface:

| Type | Direction | R object |
| ---- | --------- | -------- |
| `polars_core::frame::DataFrame` | `IntoArrowRobj` | `nanoarrow_array_stream` |
| `polars_core::frame::DataFrame` | `FromArrowRobj` | `nanoarrow_array_stream` |
| `polars_arrow::ffi::ArrowArrayStream` | `IntoArrowRobj` | `nanoarrow_array_stream` |
| `polars_arrow::ffi::ArrowArrayStreamReader` | `FromArrowRobj` | `nanoarrow_array_stream` |

### Example: round-trip a Polars DataFrame through R

```rust
use extendr_api::prelude::*;
use anyhow::anyhow;
use arrow_extendr::{FromArrowRobj, IntoArrowRobj};
use polars_core::frame::DataFrame;

#[extendr]
/// @export
fn polars_round_trip(x: Robj) -> anyhow::Result<Robj> {
    let df = DataFrame::from_arrow_robj(&x)?;
    rprintln!("{df:?}");
    df.into_arrow_robj().map_err(|e| anyhow!("{e:?}"))
}
```

```r
library(nanoarrow)

df <- data.frame(a = 1:5, b = letters[1:5])
stream <- as_nanoarrow_array_stream(df)
polars_round_trip(stream)
```

## Using arrow-extendr in a package

To use arrow-extendr in an R package first create an R package and make it an extendr package with:

```r
usethis::create_package("my_package")
rextendr::use_extendr()
```

arrow-extendr implements all Arrow C Data Interface pointer handling in pure Rust, so the `{nanoarrow}` R package is **not** required as a runtime dependency of your package. The pointer objects (`nanoarrow_array`, `nanoarrow_schema`, `nanoarrow_array_stream`) are standard R external pointers whose class names are set by arrow-extendr directly.

## Versioning

At present, versions of arrow-rs are not compatible with each other. This means if your crate uses arrow-rs version `48.0.1`, then the arrow-extendr must also use that same version. As such, arrow-extendr uses the same versions as arrow-rs so that it is easy to match the required versions you need.

**Versions**:

- 58.0.0
- 55.1.0
- 54.0.0
- 53.0.0
- 52.0.0
- 51.0.0
- 50.0.0 (compatible with geoarrow-rs 0.1.0)
- 49.0.0-geoarrow (not available on crates.io but is the current Git version)
- 48.0.1
- 49.0.0
