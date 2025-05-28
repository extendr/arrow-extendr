# arrow_extendr

arrow-extendr is a crate that facilitates the transfer of [Apache Arrow](https://arrow.apache.org/) memory between R and Rust. It utilizes [extendr](https://extendr.github.io/), the [**`{nanoarrow}`**](https://arrow.apache.org/nanoarrow/0.3.0/r/index.html) R package, and [arrow-rs](https://docs.rs/arrow).

## Versioning

At present, versions of arrow-rs are not compatible with each other. This means if your crate uses arrow-rs version `48.0.1`, then the arrow-extendr must also use that same version. As such, arrow-extendr uses the same versions as arrow-rs so that it is easy to match the required versions you need.

**Versions**:

- 55.1.0
- 54.0.0
- 53.0.0
- 52.0.0
- 51.0.0
- 50.0.0 (compatible with geoarrow-rs 0.1.0)
- 49.0.0-geoarrow (not available on crates.io but is the current Git version)
- 48.0.1
- 49.0.0

### Motivating Example

Say we have the following `DBI` connection which we will send requests to using arrow.
The result of `dbGetQueryArrow()` is a `nanoarrow_array_stream`. We want to
count the number of rows in each batch of the steam using Rust.

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
use arrow_extendr::from::FromArrowRobj;
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

## Using arrow-extendr in a package

To use arrow-extendr in an R package first create an R package and make it an extendr package with:

```r
usethis::create_package("my_package")
rextendr::use_extendr();
```

Next, you have to ensure that `nanoarrow` is a dependency of the package since arrow-extendr will call functions from nanoarrow to convert between R and Arrow memory. To do this run `usethis::use_package("nanoarrow")` to add it to your Imports field in the DESCRIPTION file.
