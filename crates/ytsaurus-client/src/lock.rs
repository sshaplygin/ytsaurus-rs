//! Locks: what Cypress gives a transaction to coordinate with.
//!
//! A lock belongs to a transaction and is released when it ends. There is no
//! way to take one without a transaction — the cluster answers `A valid master
//! transaction is required` — so [`Client::lock`](crate::Client::lock) refuses
//! before asking when the client is not in one.
//!
//! Writing to a node takes an exclusive lock on its own; taking one explicitly
//! is for saying *now* rather than later, which is what makes it a coordination
//! primitive rather than an implementation detail.

/// What a lock leaves other transactions able to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Nobody else may write to the node, and nobody else may take this lock.
    ///
    /// What a writer takes, and what the cluster takes on your behalf when you
    /// write. Two transactions asking for it is the conflict worth designing
    /// around: the loser is told which transaction won.
    Exclusive,

    /// Others may take this lock too; nobody may take an exclusive one.
    ///
    /// Several writers of *different* parts of a node — different children of a
    /// map node, different attributes — rather than several writers of the same
    /// thing.
    Shared,

    /// Pins the node as it is now, for this transaction to read.
    ///
    /// Compatible with somebody else's exclusive lock: they carry on writing,
    /// and this transaction goes on seeing the version it pinned. What a long
    /// read of a table that others keep replacing wants.
    Snapshot,
}

impl LockMode {
    /// The spelling the cluster expects.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            LockMode::Exclusive => "exclusive",
            LockMode::Shared => "shared",
            LockMode::Snapshot => "snapshot",
        }
    }
}

/// A lock held by a transaction.
///
/// Released when the transaction ends, whichever way it ends. There is no
/// method to drop it here: the lock's lifetime is the transaction's, which is
/// the only thing that makes it safe to hold one across a failure.
///
/// The cluster also answers with a revision, which is `0` for a lock that is
/// still queued and therefore says less than it appears to. It is left out
/// rather than exposed as a number that means two different things.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lock {
    /// The lock's own ID. `#<id>/@state` is how the cluster reports its state.
    pub id: String,
    /// The node it is held on.
    pub node_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_are_spelled_the_way_the_cluster_spells_them() {
        assert_eq!(LockMode::Exclusive.as_str(), "exclusive");
        assert_eq!(LockMode::Shared.as_str(), "shared");
        assert_eq!(LockMode::Snapshot.as_str(), "snapshot");
    }
}
