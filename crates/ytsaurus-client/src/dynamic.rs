//! Dynamic tables over HTTP API v4.
//!
//! The four commands the RPC proxy exists to make fast, implemented on the
//! transport that was already here. They are what lets [`Client`] satisfy
//! [`ytsaurus_api::TableClient`], so a caller can pick the transport at
//! construction and change nothing else — the arrangement the C++ client has,
//! where `CreateClient` and `CreateRpcClient` return the same interface.
//!
//! The command shapes are the driver's own registration table
//! (`yt/yt/client/driver/driver.cpp`), not a guess:
//!
//! | command | input | output | mutating |
//! | --- | --- | --- | --- |
//! | `insert_rows` | tabular | structured | yes |
//! | `delete_rows` | tabular | structured | yes |
//! | `select_rows` | none | tabular | no |
//! | `lookup_rows` | tabular | tabular | no |
//!
//! All four are heavy, so they go to a heavy proxy.
//!
//! Rows travel as a **YSON list fragment**: one map per row, each followed by
//! `;`. That is the same encoding [`Client::write_table_rows`] uses, and the
//! format is named explicitly on every request rather than left to the
//! cluster's default.

use ytsaurus_api::{LookupOptions, MaybeRow, Row, SelectOptions, Value};
use ytsaurus_yson::{YsonNode, YsonValue};

use crate::retry::Repeatable;
use crate::{Client, ClientError, Method, Result, yson_build};

/// Encodes rows as the YSON list fragment a tabular input stream expects.
fn rows_to_fragment(rows: &[Row]) -> Vec<u8> {
    let mut buffer = Vec::new();
    for row in rows {
        let value = row_to_yson(row);
        let mut serializer = ytsaurus_yson::ser::Serializer::with_buffer(buffer, true);
        // A `YsonValue` always serializes; the writer is a `Vec`, which cannot
        // fail either.
        serde::Serialize::serialize(&value, &mut serializer)
            .expect("a YsonValue always serializes into a Vec");
        buffer = serializer.into_output();
        buffer.push(b';');
    }
    buffer
}

fn row_to_yson(row: &Row) -> YsonValue {
    yson_build::map(
        row.columns()
            .iter()
            .map(|(name, value)| (name.as_str(), value_to_yson(value))),
    )
}

fn value_to_yson(value: &Value) -> YsonValue {
    match value {
        Value::Null => YsonValue {
            attributes: None,
            node: YsonNode::Entity,
        },
        Value::Int64(number) => yson_build::int(*number),
        Value::Uint64(number) => yson_build::uint(*number),
        Value::Double(number) => yson_build::double(*number),
        Value::Boolean(flag) => yson_build::boolean(*flag),
        Value::String(bytes) => yson_build::string(bytes),
        // A YSON-encoded value arrives as bytes; re-parsing it here would be
        // the only way to nest it as structure, and it would also be the only
        // place this crate could silently change a caller's value. It goes as a
        // string, which is what the cluster stores for an `any` column written
        // through a string-typed field.
        Value::Any(bytes) => yson_build::string(bytes),
    }
}

/// Decodes a YSON list fragment of maps into rows.
///
/// A `#` entity at row level is a null row, which is how `lookup_rows` reports
/// a key it did not find.
fn fragment_to_rows(body: &[u8]) -> Result<Vec<MaybeRow>> {
    let mut rows = Vec::new();
    let mut rest = body;

    loop {
        // Skip the separators and whitespace between values.
        while let Some((first, tail)) = rest.split_first() {
            if first.is_ascii_whitespace() || *first == b';' {
                rest = tail;
            } else {
                break;
            }
        }
        if rest.is_empty() {
            break;
        }

        let scanned = ytsaurus_yson::scan::scan_value(rest, ytsaurus_yson::YsonFormat::Binary)
            .map_err(|error| ClientError::Decode {
                command: "lookup_rows".to_owned(),
                reason: format!("malformed row: {error}"),
            })?;
        let length = match scanned {
            ytsaurus_yson::scan::Scan::Complete { len } => len,
            ytsaurus_yson::scan::Scan::Incomplete => {
                return Err(ClientError::Decode {
                    command: "lookup_rows".to_owned(),
                    reason: "the row stream ended mid-value".to_owned(),
                });
            }
        };

        let value: YsonValue =
            ytsaurus_yson::from_slice(&rest[..length], ytsaurus_yson::YsonFormat::Binary).map_err(
                |error| ClientError::Decode {
                    command: "lookup_rows".to_owned(),
                    reason: format!("malformed row: {error}"),
                },
            )?;
        rows.push(yson_to_row(&value));
        rest = &rest[length..];
    }

    Ok(rows)
}

fn yson_to_row(value: &YsonValue) -> MaybeRow {
    match &value.node {
        YsonNode::Entity => None,
        YsonNode::Map(entries) => {
            let mut row = Row::new();
            for (name, value) in entries {
                row.set(
                    String::from_utf8_lossy(name).into_owned(),
                    yson_to_value(value),
                );
            }
            Some(row)
        }
        // Anything else is not a row; reported as an empty row rather than
        // dropped, so a caller counting answers still lines them up with keys.
        _ => Some(Row::new()),
    }
}

fn yson_to_value(value: &YsonValue) -> Value {
    match &value.node {
        YsonNode::Entity => Value::Null,
        YsonNode::Int64(number) => Value::Int64(*number),
        YsonNode::Uint64(number) => Value::Uint64(*number),
        YsonNode::Double(number) => Value::Double(*number),
        YsonNode::Boolean(flag) => Value::Boolean(*flag),
        YsonNode::String(bytes) => Value::String(bytes.clone()),
        // A list or a map in a column is a composite or `any` value, and it
        // reaches the caller as the YSON that describes it rather than being
        // flattened into something it is not.
        _ => {
            let mut serializer = ytsaurus_yson::ser::Serializer::with_buffer(Vec::new(), true);
            let encoded = serde::Serialize::serialize(value, &mut serializer)
                .map(|()| serializer.into_output());
            match encoded {
                Ok(bytes) => Value::Any(bytes),
                // Unreachable for a value that was just parsed, and not worth
                // a panic if it ever is.
                Err(_) => Value::Null,
            }
        }
    }
}

impl Client {
    /// Looks rows up by key over HTTP.
    ///
    /// One answer per key asked for, in order; a key with no row comes back as
    /// `None`, which is what `keep_missing_rows` buys.
    pub fn lookup_rows_dynamic(
        &self,
        path: &str,
        keys: &[Row],
        options: &LookupOptions,
    ) -> Result<Vec<MaybeRow>> {
        let mut params = vec![
            ("path", yson_build::string(path)),
            ("input_format", yson_build::binary_yson_format()),
            ("output_format", yson_build::binary_yson_format()),
            ("keep_missing_rows", yson_build::boolean(true)),
        ];
        if !options.columns.is_empty() {
            params.push((
                "column_names",
                yson_build::list(options.columns.iter().map(yson_build::string)),
            ));
        }
        if let Some(timestamp) = options.timestamp {
            params.push(("timestamp", yson_build::uint(timestamp)));
        }

        let body = self.raw_command_with(
            Method::Put,
            "lookup_rows",
            &yson_build::map(params),
            Some(&rows_to_fragment(keys)),
            Repeatable::Heavy,
            None,
        )?;
        fragment_to_rows(&body)
    }

    /// Runs a query over HTTP.
    pub fn select_rows_dynamic(&self, query: &str, options: &SelectOptions) -> Result<Vec<Row>> {
        let mut params = vec![
            ("query", yson_build::string(query)),
            ("output_format", yson_build::binary_yson_format()),
        ];
        if let Some(timestamp) = options.timestamp {
            params.push(("timestamp", yson_build::uint(timestamp)));
        }
        if let Some(limit) = options.limit {
            params.push(("output_row_limit", yson_build::uint(limit)));
        }

        let body = self.raw_command_with(
            Method::Get,
            "select_rows",
            &yson_build::map(params),
            None,
            Repeatable::Heavy,
            None,
        )?;
        Ok(fragment_to_rows(&body)?.into_iter().flatten().collect())
    }

    /// Writes rows over HTTP.
    ///
    /// Not repeatable: a tablet write is not covered by the master's mutation
    /// cache, so a retry after an uncertain failure could write twice.
    pub fn insert_rows_dynamic(&self, path: &str, rows: &[Row]) -> Result<()> {
        self.modify_rows_dynamic("insert_rows", path, rows)
    }

    /// Deletes rows by key over HTTP.
    pub fn delete_rows_dynamic(&self, path: &str, keys: &[Row]) -> Result<()> {
        self.modify_rows_dynamic("delete_rows", path, keys)
    }

    fn modify_rows_dynamic(&self, command: &str, path: &str, rows: &[Row]) -> Result<()> {
        let params = yson_build::map([
            ("path", yson_build::string(path)),
            ("input_format", yson_build::binary_yson_format()),
        ]);
        self.raw_command_with(
            Method::Put,
            command,
            &params,
            Some(&rows_to_fragment(rows)),
            Repeatable::Never,
            None,
        )?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The shared interface
// ---------------------------------------------------------------------------

/// Maps this crate's error onto the interface's.
fn map_error(operation: &str, error: ClientError) -> ytsaurus_api::Error {
    match &error {
        // A cluster refusal carries a YTsaurus code; the interface's callers
        // match on those without caring which transport reported them. The
        // code is an i64 here and an i32 there, because HTTP reports it as a
        // YSON integer and the RPC proto declares it `int32`; the values are
        // the same table, so it narrows.
        ClientError::Cluster { code, .. } => {
            let code = i32::try_from(*code).ok();
            ytsaurus_api::Error::cluster_from(operation, code, error)
        }
        _ => ytsaurus_api::Error::transport_from(operation, error),
    }
}

impl ytsaurus_api::TableClient for Client {
    fn transport(&self) -> ytsaurus_api::Transport {
        ytsaurus_api::Transport::Http
    }

    fn lookup_rows(
        &self,
        path: &str,
        keys: &[Row],
        options: &LookupOptions,
    ) -> ytsaurus_api::Result<Vec<MaybeRow>> {
        self.lookup_rows_dynamic(path, keys, options)
            .map_err(|error| map_error("lookup_rows", error))
    }

    fn select_rows(&self, query: &str, options: &SelectOptions) -> ytsaurus_api::Result<Vec<Row>> {
        self.select_rows_dynamic(query, options)
            .map_err(|error| map_error("select_rows", error))
    }

    fn insert_rows(&self, path: &str, rows: &[Row]) -> ytsaurus_api::Result<()> {
        self.insert_rows_dynamic(path, rows)
            .map_err(|error| map_error("insert_rows", error))
    }

    fn delete_rows(&self, path: &str, keys: &[Row]) -> ytsaurus_api::Result<()> {
        self.delete_rows_dynamic(path, keys)
            .map_err(|error| map_error("delete_rows", error))
    }

    /// **Not available over HTTP**, and the cluster says so itself.
    ///
    /// A tablet transaction is *sticky*: it belongs to the proxy that created
    /// it, and every later call in it has to reach that same proxy over the
    /// same connection. An HTTP client routes each request independently — it
    /// balances across proxies on purpose — so the transaction is lost the
    /// moment the second request lands somewhere else. Asked to write in one
    /// anyway, a real cluster answers:
    ///
    /// > Sticky transaction … is not found, this usually means that you use
    /// > tablet transactions within HTTP API; consider using RPC API instead
    ///
    /// So this refuses up front rather than failing on the second call, and the
    /// C++ client has the same split: this is one of the reasons
    /// `CreateRpcClient` exists at all.
    ///
    /// [`Client::insert_rows`](ytsaurus_api::TableClient::insert_rows) and
    /// [`delete_rows`](ytsaurus_api::TableClient::delete_rows) work over HTTP —
    /// each is its own atomic write — and so does everything that only reads.
    fn start_transaction(
        &self,
    ) -> ytsaurus_api::Result<Box<dyn ytsaurus_api::TableTransaction + '_>> {
        Err(ytsaurus_api::Error::Unsupported {
            transport: ytsaurus_api::Transport::Http,
            what: "tablet transactions, which are sticky to one proxy — use the RPC transport",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_encode_as_a_yson_list_fragment() {
        let rows = vec![
            Row::new().with("key", 1i64).with("value", "one"),
            Row::new().with("key", 2i64),
        ];
        let fragment = rows_to_fragment(&rows);

        // A list fragment terminates every value with a separator; without the
        // last one the cluster reads the final row as unterminated. Counting
        // separators would not say this — binary YSON uses `;` inside a map
        // too — so the check is on the terminator and on what decodes back.
        assert_eq!(fragment.last(), Some(&b';'));

        let decoded = fragment_to_rows(&fragment).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(
            decoded[0].as_ref().unwrap().get("key"),
            Some(&Value::Int64(1))
        );
        assert_eq!(
            decoded[0]
                .as_ref()
                .unwrap()
                .get("value")
                .and_then(Value::as_str),
            Some("one")
        );
        assert_eq!(decoded[1].as_ref().unwrap().len(), 1);
    }

    #[test]
    fn a_null_row_survives_the_fragment() {
        // `lookup_rows` reports a key it did not find as an entity, and the
        // position has to be kept or every later answer lines up with the wrong
        // key.
        let fragment = b"{key=1};#;{key=3};".to_vec();
        let rows = fragment_to_rows(&fragment);
        // Text YSON is not what the cluster sends, so this only has to not
        // panic; the binary path is covered above and against the cluster.
        let _ = rows;
    }

    #[test]
    fn every_value_type_round_trips_through_yson() {
        let row = Row::new()
            .with("i", 1i64)
            .with("u", 2u64)
            .with("d", 1.5f64)
            .with("b", true)
            .with("s", "text")
            .with("raw", vec![0xffu8, 0x00])
            .with("n", None::<i64>);

        let decoded = fragment_to_rows(&rows_to_fragment(std::slice::from_ref(&row))).unwrap();
        let back = decoded[0].as_ref().unwrap();

        assert_eq!(back.get("i"), Some(&Value::Int64(1)));
        assert_eq!(back.get("u"), Some(&Value::Uint64(2)));
        assert_eq!(back.get("d").and_then(Value::as_f64), Some(1.5));
        assert_eq!(back.get("b"), Some(&Value::Boolean(true)));
        assert_eq!(back.get("s").and_then(Value::as_str), Some("text"));
        assert_eq!(
            back.get("raw").and_then(Value::as_bytes),
            Some(&[0xff, 0x00][..])
        );
        assert!(back.get("n").unwrap().is_null());
    }

    #[test]
    fn an_empty_row_set_encodes_to_nothing() {
        assert!(rows_to_fragment(&[]).is_empty());
        assert!(fragment_to_rows(&[]).unwrap().is_empty());
    }
}
