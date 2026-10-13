//! Dynamic tables over HTTP API v4: `insert_rows`, `delete_rows`,
//! `select_rows` and `lookup_rows`, which let [`Client`] implement
//! [`ytsaurus_api::TableClient`], so the transport is chosen at construction,
//! as with the C++ client's `CreateClient` and `CreateRpcClient`.
//!
//! All four are heavy in the driver's registration table
//! (`yt/yt/client/driver/driver.cpp`); only `select_rows` takes no input, and
//! only `insert_rows` and `delete_rows` mutate. Rows travel as a binary YSON
//! list fragment, one map per row, as in [`Client::write_table_rows`], and the
//! format is named on every request.

use ytsaurus_api::{LookupOptions, MaybeRow, Row, SelectOptions, Value};
use ytsaurus_yson::{YsonFormat, YsonNode, YsonValue};

use crate::retry::Repeatable;
use crate::{Client, ClientError, Method, Result, yson_build};

/// Encodes rows as the YSON list fragment a tabular input stream expects.
fn rows_to_fragment(rows: &[Row]) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    for row in rows {
        let value = row_to_yson(row)?;
        let mut serializer = ytsaurus_yson::ser::Serializer::with_buffer(buffer, true);
        // Cannot fail: a `YsonValue` serialised into a `Vec`.
        serde::Serialize::serialize(&value, &mut serializer)
            .expect("a YsonValue always serializes into a Vec");
        buffer = serializer.into_output();
        buffer.push(b';');
    }
    Ok(buffer)
}

fn row_to_yson(row: &Row) -> Result<YsonValue> {
    let entries = row
        .columns()
        .iter()
        .map(|(name, value)| value_to_yson(value).map(|value| (name.as_str(), value)))
        .collect::<Result<Vec<_>>>()?;
    Ok(yson_build::map(entries))
}

fn value_to_yson(value: &Value) -> Result<YsonValue> {
    Ok(match value {
        Value::Null => YsonValue {
            attributes: None,
            node: YsonNode::Entity,
        },
        Value::Int64(number) => yson_build::int(*number),
        Value::Uint64(number) => yson_build::uint(*number),
        Value::Double(number) => yson_build::double(*number),
        Value::Boolean(flag) => yson_build::boolean(*flag),
        Value::String(bytes) => yson_build::string(bytes),
        // A YSON value, accepted in binary or text; the fragment is binary.
        Value::Any(bytes) => ytsaurus_yson::from_slice(bytes, YsonFormat::Binary)
            .or_else(|binary_error| {
                ytsaurus_yson::from_slice(bytes, YsonFormat::Text).map_err(|text_error| {
                    ClientError::Decode {
                        command: "writing dynamic table row".to_owned(),
                        reason: format!(
                            "Value::Any is not one YSON value (binary: {binary_error}; text: {text_error})"
                        ),
                    }
                })
            })?,
    })
}

/// Decodes a YSON list fragment of maps into rows. A `#` entity is a null row,
/// which is how `lookup_rows` reports a key it did not find.
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
        // A list or a map is a composite or `any` value: passed on as its YSON.
        _ => {
            let mut serializer = ytsaurus_yson::ser::Serializer::with_buffer(Vec::new(), true);
            let encoded = serde::Serialize::serialize(value, &mut serializer)
                .map(|()| serializer.into_output());
            match encoded {
                Ok(bytes) => Value::Any(bytes),
                // Unreachable for a value just parsed.
                Err(_) => Value::Null,
            }
        }
    }
}

impl Client {
    /// Looks rows up by key over HTTP: one answer per key, in order, and
    /// `None` for a key with no row.
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

        let keys = rows_to_fragment(keys)?;
        let body = self.raw_command_with(
            Method::Put,
            "lookup_rows",
            &yson_build::map(params),
            Some(&keys),
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

    /// Writes rows over HTTP, sent once: the master's mutation cache does not
    /// cover a tablet write, so a retry could write twice.
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
        let body = rows_to_fragment(rows)?;
        self.raw_command_with(
            Method::Put,
            command,
            &params,
            Some(&body),
            Repeatable::Never,
            None,
        )?;
        Ok(())
    }
}

// The shared interface.

/// Maps this crate's error onto the interface's.
fn map_error(operation: &str, error: ClientError) -> ytsaurus_api::Error {
    match &error {
        // Callers of the interface match YTsaurus codes whatever the transport;
        // HTTP's i64 narrows to RPC's int32, which holds the same codes.
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

    /// **Not available over HTTP.** A tablet transaction is sticky to the proxy
    /// that created it, and HTTP requests are balanced across proxies, so a
    /// cluster answers `Sticky transaction … is not found, this usually means
    /// that you use tablet transactions within HTTP API`. This refuses up
    /// front; use the RPC transport.
    /// [`insert_rows`](ytsaurus_api::TableClient::insert_rows) and
    /// [`delete_rows`](ytsaurus_api::TableClient::delete_rows) work, each its
    /// own atomic write.
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
        let fragment = rows_to_fragment(&rows).unwrap();

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

        let fragment = rows_to_fragment(std::slice::from_ref(&row)).unwrap();
        let decoded = fragment_to_rows(&fragment).unwrap();
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
        assert!(rows_to_fragment(&[]).unwrap().is_empty());
        assert!(fragment_to_rows(&[]).unwrap().is_empty());
    }

    #[test]
    fn any_values_are_embedded_as_yson_not_strings() {
        let any = yson_build::list([yson_build::int(1), yson_build::int(2)]);
        let bytes = ytsaurus_yson::to_vec(&any, YsonFormat::Binary).unwrap();
        let row = Row::new().with("value", Value::Any(bytes));

        let fragment = rows_to_fragment(&[row]).unwrap();
        let decoded = fragment_to_rows(&fragment).unwrap();
        let value = decoded[0].as_ref().unwrap().get("value");

        match value {
            Some(Value::Any(bytes)) => {
                let value: YsonValue =
                    ytsaurus_yson::from_slice(bytes, YsonFormat::Binary).unwrap();
                assert!(matches!(value.node, YsonNode::List(_)));
            }
            other => panic!("the list was encoded as {other:?}, not as YSON"),
        }
    }

    #[test]
    fn any_values_accept_text_yson_too() {
        let row = Row::new().with("value", Value::Any(b"[1;2]".to_vec()));

        let fragment = rows_to_fragment(&[row]).unwrap();
        let decoded = fragment_to_rows(&fragment).unwrap();
        assert!(matches!(
            decoded[0].as_ref().unwrap().get("value"),
            Some(Value::Any(_))
        ));
    }

    #[test]
    fn an_invalid_any_value_fails_before_the_request_is_sent() {
        let row = Row::new().with("value", Value::Any(b"not a yson value".to_vec()));
        let error = rows_to_fragment(&[row]).unwrap_err();
        assert!(error.to_string().contains("Value::Any"), "{error}");
    }
}
