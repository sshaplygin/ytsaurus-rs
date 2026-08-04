//! `shards` — a vanilla job: no input table, work decided by the job's cookie.
//!
//! A vanilla operation runs jobs that are not a transformation of a table.
//! Nothing arrives on fd 0, so each job has to work out its own share of the
//! work — `YT_JOB_COOKIE` is what it divides by, and it is stable across a
//! restart, so a retried job redoes its own shard rather than someone else's.
//!
//! This one computes a slice of a sum and writes the result to its output
//! table, which is the smallest thing that shows the shape: several jobs, no
//! input, coordinated by nothing but arithmetic.
//!
//! ```sh
//! scripts/build-worker.sh shards
//! cargo run -p ytsaurus-client --example vanilla
//! ```

use serde::Serialize;
use ytsaurus_job::{JobWriter, job_cookie};

/// How many numbers the operation adds up between all its jobs.
const NUMBERS: u64 = 1_000;

/// One job's report.
#[derive(Serialize)]
struct Shard {
    cookie: i64,
    shards: i64,
    /// Sum of the numbers this job was responsible for.
    sum: i64,
    /// How many it added, so the launcher can check the whole range was covered.
    counted: i64,
}

fn main() {
    ytsaurus_job::run(|| {
        // No JobReader: there is no input stream to read.
        let mut writer = JobWriter::descriptors(1)?;

        let cookie = job_cookie().unwrap_or(0);
        let shards = shard_count();

        eprintln!("shards: job {cookie} of {shards}");

        let (sum, counted) = shard_sum(cookie, shards);
        writer.write(
            0,
            &Shard {
                cookie: cookie as i64,
                shards: shards as i64,
                sum,
                counted,
            },
        )?;

        writer.finish()
    })
}

/// How many jobs the task was given, from the command line.
///
/// The cluster tells a job its cookie but not, outside a gang operation, how
/// many siblings it has — so the launcher passes it in the command, which is
/// the ordinary way a vanilla task is parameterised.
fn shard_count() -> u64 {
    std::env::args()
        .nth(1)
        .and_then(|n| n.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1)
}

/// Sums the numbers in `1..=NUMBERS` that belong to this shard.
fn shard_sum(cookie: u64, shards: u64) -> (i64, i64) {
    let mut sum = 0_i64;
    let mut counted = 0_i64;

    for n in 1..=NUMBERS {
        if n % shards == cookie % shards {
            sum += n as i64;
            counted += 1;
        }
    }
    (sum, counted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shards_together_cover_the_whole_range() {
        for shards in [1_u64, 2, 3, 7] {
            let total: i64 = (0..shards).map(|c| shard_sum(c, shards).0).sum();
            let counted: i64 = (0..shards).map(|c| shard_sum(c, shards).1).sum();

            let expected = (NUMBERS * (NUMBERS + 1) / 2) as i64;
            assert_eq!(
                total, expected,
                "{shards} shards must sum to the same total"
            );
            assert_eq!(counted, NUMBERS as i64, "every number counted exactly once");
        }
    }

    #[test]
    fn a_shard_count_of_zero_is_treated_as_one() {
        // `shard_count` never returns 0, because `n % 0` would panic in a job
        // where nothing is watching.
        assert_eq!(shard_sum(0, 1).1, NUMBERS as i64);
    }
}
