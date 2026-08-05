# ytsaurus-format

`DataFormat` is the shared public format selection for `ytsaurus-client` and
`ytsaurus-job`:

```rust
use ytsaurus_format::DataFormat;

let format = DataFormat::binary_yson();
```

It currently represents binary/text YSON and validated Skiff declarations. The
non-exhaustive enum is the single extension point for future YTsaurus formats.
