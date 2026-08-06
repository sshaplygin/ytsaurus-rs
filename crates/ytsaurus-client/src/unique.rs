//! One source of ids, for the two things in this crate that need them.
//!
//! A [`MutationId`](crate::MutationId) the cluster deduplicates a repeated
//! command by, and the trace and span ids in a
//! [`TraceContext`](crate::TraceContext). They want the same thing from an id
//! and they wanted it in the same way, so they ask for it in one place: two
//! copies of this would mean the uniqueness argument had to stay true twice,
//! and a fix to one of them — a different clock, a real random source, the
//! truncation below — would land in one and not the other.

use std::sync::atomic::{AtomicU64, Ordering};

/// Counts calls, so two words drawn in the same nanosecond still differ.
static WORDS: AtomicU64 = AtomicU64::new(0);

/// Sixty-four bits unlikely to have been produced before.
///
/// The entropy comes from `RandomState`, which the standard library seeds from
/// the OS once per process, mixed with a counter and the clock. Both callers
/// need an id to be *unique*, not unpredictable, and that is a poor reason to
/// add a random-number crate to a dependency list this short.
///
/// `salt` separates the several words that make up one id. A fresh
/// `RandomState` per call already has its own keys, so two words are not two
/// views of one 64-bit value; the salt says so at the call site as well.
pub(crate) fn word(salt: u64) -> u64 {
    use std::hash::{BuildHasher, Hasher, RandomState};

    let counter = WORDS.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);

    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(counter);
    hasher.write_u64(nanos);
    hasher.write_u64(salt);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_do_not_repeat() {
        // The whole of what either caller needs. Drawn from one thread and
        // with the same salt, which is the case the counter exists for: the
        // clock alone does not separate two calls in the same nanosecond.
        let drawn: std::collections::HashSet<u64> = (0..100_000).map(|_| word(0)).collect();
        assert_eq!(drawn.len(), 100_000, "a word was drawn twice");
    }

    #[test]
    fn the_salt_separates_words_of_one_id() {
        let together: std::collections::HashSet<u64> = (0..4).map(word).collect();
        assert_eq!(together.len(), 4, "two words of one id collided");
    }
}
