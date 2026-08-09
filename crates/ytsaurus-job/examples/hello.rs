//! Smoke-test worker: proves the fully static musl build pipeline works.
//!
//! Build it the way a real worker is built:
//!
//! ```sh
//! cargo build -p ytsaurus-job --example hello \
//!     --profile release-worker --target x86_64-unknown-linux-musl
//! file target/x86_64-unknown-linux-musl/release-worker/hello
//! ```

fn main() {
    println!("hello from a statically linked ytsaurus-rs worker");
}
