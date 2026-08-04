#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use ytsaurus_yson::WithAttributes;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) struct User {
    pub(crate) name: String,
    pub(crate) age: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) struct Meta {
    pub(crate) active: bool,
    pub(crate) role: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) enum UserStatus {
    Pending,
    Active,
    Banned(String),
    Custom { code: u32, reason: String },
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) struct ComplexEntity {
    pub(crate) id: u64,
    pub(crate) user: WithAttributes<User, Meta>,
    pub(crate) tags: Vec<String>,
    pub(crate) status: Option<UserStatus>,
}
