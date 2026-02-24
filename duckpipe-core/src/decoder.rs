//! pgoutput binary protocol decoder.
//!
//! Parses the pgoutput wire format (protocol version 1) into structured types.
//! This module is pure Rust with no PostgreSQL dependencies, making it usable
//! in both the pgrx background worker and a standalone daemon.

use crate::types::{RelCacheEntry, Value};

// --- Low-level binary readers ---

pub fn read_byte(data: &[u8], cursor: &mut usize) -> u8 {
    let b = data[*cursor];
    *cursor += 1;
    b
}

pub fn read_i16(data: &[u8], cursor: &mut usize) -> i16 {
    let val = i16::from_be_bytes([data[*cursor], data[*cursor + 1]]);
    *cursor += 2;
    val
}

pub fn read_i32(data: &[u8], cursor: &mut usize) -> i32 {
    let val = i32::from_be_bytes([
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
    ]);
    *cursor += 4;
    val
}

pub fn read_i64(data: &[u8], cursor: &mut usize) -> i64 {
    let val = i64::from_be_bytes([
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
        data[*cursor + 4],
        data[*cursor + 5],
        data[*cursor + 6],
        data[*cursor + 7],
    ]);
    *cursor += 8;
    val
}

pub fn read_string(data: &[u8], cursor: &mut usize) -> String {
    let start = *cursor;
    while *cursor < data.len() && data[*cursor] != 0 {
        *cursor += 1;
    }
    let s = String::from_utf8_lossy(&data[start..*cursor]).to_string();
    if *cursor < data.len() {
        *cursor += 1; // skip null terminator
    }
    s
}

pub fn read_bytes(data: &[u8], cursor: &mut usize, n: usize) -> Vec<u8> {
    let result = data[*cursor..*cursor + n].to_vec();
    *cursor += n;
    result
}

// --- High-level message parsers ---

/// Parse a RELATION ('R') message body (after the type byte).
/// Returns (relation_id, RelCacheEntry).
pub fn parse_relation_message(data: &[u8], cursor: &mut usize) -> (u32, RelCacheEntry) {
    let rel_id = read_i32(data, cursor) as u32;
    let nspname = read_string(data, cursor);
    let relname = read_string(data, cursor);
    let _replica_identity = read_byte(data, cursor);
    let natts = read_i16(data, cursor) as usize;

    let mut attnames = Vec::with_capacity(natts);
    let mut attkeys = Vec::new();
    let mut atttypes = Vec::with_capacity(natts);

    for i in 0..natts {
        let flags = read_byte(data, cursor);
        let attname = read_string(data, cursor);
        let type_oid = read_i32(data, cursor) as u32;
        let _type_mod = read_i32(data, cursor);

        attnames.push(attname);
        atttypes.push(type_oid);
        if flags & 1 != 0 {
            attkeys.push(i);
        }
    }

    (
        rel_id,
        RelCacheEntry {
            nspname,
            relname,
            attnames,
            attkeys,
            atttypes,
        },
    )
}

/// Parse a pgoutput text representation into a typed Value using the column's type OID.
/// Falls back to Text(String) for unrecognized types — DuckDB auto-casts at insert time.
fn parse_text_value(text: &str, type_oid: u32) -> Value {
    match type_oid {
        16 => match text {
            // bool
            "t" => Value::Bool(true),
            "f" => Value::Bool(false),
            _ => Value::Text(text.to_string()),
        },
        21 => text
            .parse::<i16>()
            .map(Value::Int16)
            .unwrap_or_else(|_| Value::Text(text.to_string())), // int2
        23 | 26 => text
            .parse::<i32>()
            .map(Value::Int32)
            .unwrap_or_else(|_| Value::Text(text.to_string())), // int4, oid
        20 => text
            .parse::<i64>()
            .map(Value::Int64)
            .unwrap_or_else(|_| Value::Text(text.to_string())), // int8
        700 => text
            .parse::<f32>()
            .map(Value::Float32)
            .unwrap_or_else(|_| Value::Text(text.to_string())), // float4
        701 => text
            .parse::<f64>()
            .map(Value::Float64)
            .unwrap_or_else(|_| Value::Text(text.to_string())), // float8
        _ => Value::Text(text.to_string()), // everything else
    }
}

/// Parse tuple data from pgoutput binary format.
/// Returns (values, unchanged_flags).
/// unchanged_flags[i] is true if column i had status 'u' (TOAST unchanged).
pub fn parse_tuple_data(
    data: &[u8],
    cursor: &mut usize,
    atttypes: &[u32],
) -> (Vec<Value>, Vec<bool>) {
    let ncols = read_i16(data, cursor) as usize;
    let mut values = Vec::with_capacity(ncols);
    let mut unchanged = Vec::with_capacity(ncols);

    for col_idx in 0..ncols {
        let kind = read_byte(data, cursor) as char;
        match kind {
            'n' => {
                values.push(Value::Null);
                unchanged.push(false);
            }
            'u' => {
                values.push(Value::Null);
                unchanged.push(true);
            }
            't' | 'b' => {
                let len = read_i32(data, cursor) as usize;
                let bytes = read_bytes(data, cursor, len);
                let text = String::from_utf8_lossy(&bytes);
                let type_oid = atttypes.get(col_idx).copied().unwrap_or(0);
                values.push(parse_text_value(&text, type_oid));
                unchanged.push(false);
            }
            _ => {
                values.push(Value::Null);
                unchanged.push(false);
            }
        }
    }
    (values, unchanged)
}

/// Extract key column values from a full row using key attribute indices.
pub fn extract_key_values(values: &[Value], key_attrs: &[usize]) -> Vec<Value> {
    key_attrs
        .iter()
        .filter_map(|&idx| {
            if idx < values.len() {
                Some(values[idx].clone())
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tuple_data_null() {
        // 1 column, status 'n' (NULL)
        let data = [0u8, 1, b'n'];
        let mut cursor = 0;
        let (values, unchanged) = parse_tuple_data(&data, &mut cursor, &[23]);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], Value::Null);
        assert!(!unchanged[0]);
    }

    #[test]
    fn test_parse_tuple_data_unchanged_toast() {
        // 1 column, status 'u' (unchanged TOAST)
        let data = [0u8, 1, b'u'];
        let mut cursor = 0;
        let (values, unchanged) = parse_tuple_data(&data, &mut cursor, &[25]);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], Value::Null);
        assert!(unchanged[0]);
    }

    #[test]
    fn test_parse_tuple_data_text() {
        // 1 column, status 't', length 5, "hello" with unknown type → Text
        let mut data = vec![0u8, 1, b't', 0, 0, 0, 5];
        data.extend_from_slice(b"hello");
        let mut cursor = 0;
        let (values, unchanged) = parse_tuple_data(&data, &mut cursor, &[25]); // text type
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], Value::Text("hello".to_string()));
        assert!(!unchanged[0]);
    }

    #[test]
    fn test_parse_tuple_data_typed_int() {
        // 1 column, status 't', length 2, "42" with int4 type → Int32
        let mut data = vec![0u8, 1, b't', 0, 0, 0, 2];
        data.extend_from_slice(b"42");
        let mut cursor = 0;
        let (values, unchanged) = parse_tuple_data(&data, &mut cursor, &[23]); // int4
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], Value::Int32(42));
        assert!(!unchanged[0]);
    }

    #[test]
    fn test_parse_text_value() {
        assert_eq!(parse_text_value("t", 16), Value::Bool(true));
        assert_eq!(parse_text_value("f", 16), Value::Bool(false));
        assert_eq!(parse_text_value("42", 21), Value::Int16(42));
        assert_eq!(parse_text_value("42", 23), Value::Int32(42));
        assert_eq!(parse_text_value("42", 20), Value::Int64(42));
        assert_eq!(parse_text_value("3.14", 700), Value::Float32(3.14));
        assert_eq!(parse_text_value("3.14", 701), Value::Float64(3.14));
        assert_eq!(
            parse_text_value("hello", 25),
            Value::Text("hello".to_string())
        );
        // Unknown type → Text
        assert_eq!(
            parse_text_value("2024-01-01", 1082),
            Value::Text("2024-01-01".to_string())
        );
    }

    #[test]
    fn test_extract_key_values() {
        let values = vec![
            Value::Int32(1),
            Value::Text("Alice".to_string()),
            Value::Int32(30),
        ];
        let keys = extract_key_values(&values, &[0]);
        assert_eq!(keys, vec![Value::Int32(1)]);

        let keys = extract_key_values(&values, &[0, 2]);
        assert_eq!(keys, vec![Value::Int32(1), Value::Int32(30)]);
    }
}
