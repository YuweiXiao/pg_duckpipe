//! Connection string parsing and conversion utilities.
//!
//! Shared by both the PG extension (for remote group DDL) and the standalone daemon.
//! Supports both libpq key=value format and PostgreSQL URI format
//! (`postgresql://user:pass@host:port/dbname?params`).

use crate::service::SlotConnectParams;

/// Parsed connection parameters for pgwire / tokio-postgres TCP connections.
pub struct ConnParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub sslmode: Option<String>,
}

/// Parse a connection string (libpq key=value or PostgreSQL URI) into individual fields.
pub fn parse_connstr(connstr: &str) -> ConnParams {
    let trimmed = connstr.trim();
    if trimmed.starts_with("postgresql://") || trimmed.starts_with("postgres://") {
        parse_uri(trimmed)
    } else {
        parse_keyvalue(trimmed)
    }
}

/// Parse a libpq-style `key=value` connection string.
///
/// Handles single-quoted values per the libpq spec: `password='my pass'`.
/// Inside quotes, backslash escapes the next character (`\'` and `\\`).
fn parse_keyvalue(connstr: &str) -> ConnParams {
    let mut host = "localhost".to_string();
    let mut port: u16 = 5432;
    let mut user = default_user();
    let mut password = String::new();
    let mut dbname = "postgres".to_string();
    let mut sslmode = None;

    for (key, value) in parse_keyvalue_pairs(connstr) {
        match key.as_str() {
            "host" | "hostaddr" => host = value,
            "port" => {
                if let Ok(p) = value.parse() {
                    port = p;
                }
            }
            "user" => user = value,
            "password" => password = value,
            "dbname" => dbname = value,
            "sslmode" => sslmode = Some(value),
            _ => {}
        }
    }

    ConnParams {
        host,
        port,
        user,
        password,
        dbname,
        sslmode,
    }
}

/// Parse libpq key=value pairs, handling single-quoted values with backslash escapes.
fn parse_keyvalue_pairs(connstr: &str) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    let mut chars = connstr.chars().peekable();

    loop {
        // Skip whitespace between pairs
        while chars.peek().map_or(false, |c| c.is_whitespace()) {
            chars.next();
        }
        if chars.peek().is_none() {
            break;
        }

        // Read key (up to '=')
        let mut key = String::new();
        while let Some(&c) = chars.peek() {
            if c == '=' {
                chars.next(); // consume '='
                break;
            }
            if c.is_whitespace() {
                break;
            }
            key.push(c);
            chars.next();
        }

        if key.is_empty() {
            break;
        }

        // Read value — may be single-quoted
        let value = if chars.peek() == Some(&'\'') {
            chars.next(); // consume opening quote
            let mut val = String::new();
            while let Some(c) = chars.next() {
                if c == '\\' {
                    // Backslash escape: next char is literal
                    if let Some(escaped) = chars.next() {
                        val.push(escaped);
                    }
                } else if c == '\'' {
                    break; // closing quote
                } else {
                    val.push(c);
                }
            }
            val
        } else {
            let mut val = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_whitespace() {
                    break;
                }
                val.push(c);
                chars.next();
            }
            val
        };

        pairs.push((key, value));
    }
    pairs
}

/// Parse a PostgreSQL URI: `postgresql://user:password@host:port/dbname?params`
fn parse_uri(uri: &str) -> ConnParams {
    let mut host = "localhost".to_string();
    let mut port: u16 = 5432;
    let mut user = default_user();
    let mut password = String::new();
    let mut dbname = "postgres".to_string();
    let mut sslmode = None;

    // Strip scheme
    let rest = uri
        .strip_prefix("postgresql://")
        .or_else(|| uri.strip_prefix("postgres://"))
        .unwrap_or(uri);

    // Split query params
    let (main, query) = rest.split_once('?').unwrap_or((rest, ""));

    // Parse query params
    for param in query.split('&') {
        if param.is_empty() {
            continue;
        }
        if let Some((k, v)) = param.split_once('=') {
            match k {
                "sslmode" => sslmode = Some(url_decode(v)),
                "user" => user = url_decode(v),
                "password" => password = url_decode(v),
                "dbname" => dbname = url_decode(v),
                "port" => {
                    if let Ok(p) = v.parse() {
                        port = p;
                    }
                }
                "host" => host = url_decode(v),
                _ => {} // skip channel_binding, application_name, etc.
            }
        }
    }

    // Split userinfo@hostspec/dbname
    let (userinfo, hostpath) = if let Some(at_pos) = main.rfind('@') {
        (&main[..at_pos], &main[at_pos + 1..])
    } else {
        ("", main)
    };

    // Parse userinfo (user:password)
    if !userinfo.is_empty() {
        if let Some((u, p)) = userinfo.split_once(':') {
            user = url_decode(u);
            password = url_decode(p);
        } else {
            user = url_decode(userinfo);
        }
    }

    // Parse host:port/dbname
    let (hostport, db) = if let Some(slash_pos) = hostpath.find('/') {
        (&hostpath[..slash_pos], &hostpath[slash_pos + 1..])
    } else {
        (hostpath, "")
    };

    if !db.is_empty() {
        dbname = url_decode(db);
    }

    if !hostport.is_empty() {
        if let Some((h, p)) = hostport.rsplit_once(':') {
            if let Ok(parsed_port) = p.parse::<u16>() {
                host = url_decode(h);
                port = parsed_port;
            } else {
                host = url_decode(hostport);
            }
        } else {
            host = url_decode(hostport);
        }
    }

    ConnParams {
        host,
        port,
        user,
        password,
        dbname,
        sslmode,
    }
}

/// Minimal percent-decoding for connection string values.
///
/// Accumulates decoded bytes and converts as UTF-8, so multi-byte sequences
/// like `%C3%A9` (é) are handled correctly.
fn url_decode(s: &str) -> String {
    let mut bytes = Vec::with_capacity(s.len());
    let mut chars = s.as_bytes().iter();
    while let Some(&b) = chars.next() {
        if b == b'%' {
            let h1 = chars.next().copied().unwrap_or(0);
            let h2 = chars.next().copied().unwrap_or(0);
            if let Ok(byte) = u8::from_str_radix(std::str::from_utf8(&[h1, h2]).unwrap_or(""), 16) {
                bytes.push(byte);
            } else {
                bytes.push(b'%');
                bytes.push(h1);
                bytes.push(h2);
            }
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).unwrap_or_else(|_| s.to_string())
}

fn default_user() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("PGUSER"))
        .unwrap_or_else(|_| "postgres".to_string())
}

/// Quote a libpq connection string value if it contains whitespace or special chars.
fn quote_connstr_value(val: &str) -> String {
    if val.is_empty() || val.contains(|c: char| c.is_whitespace() || c == '\'' || c == '\\') {
        let escaped = val.replace('\\', "\\\\").replace('\'', "\\'");
        format!("'{}'", escaped)
    } else {
        val.to_string()
    }
}

/// Build a tokio-postgres connection string from ConnParams.
///
/// Values containing whitespace, quotes, or backslashes are properly
/// single-quoted per the libpq connection string spec.
pub fn build_tokio_pg_connstr(params: &ConnParams) -> String {
    let mut parts = vec![
        format!("host={}", quote_connstr_value(&params.host)),
        format!("port={}", params.port),
        format!("user={}", quote_connstr_value(&params.user)),
        format!("dbname={}", quote_connstr_value(&params.dbname)),
    ];
    if !params.password.is_empty() {
        parts.push(format!(
            "password={}",
            quote_connstr_value(&params.password)
        ));
    }
    if let Some(ref mode) = params.sslmode {
        parts.push(format!("sslmode={}", quote_connstr_value(mode)));
    }
    parts.join(" ")
}

/// Convert ConnParams to SlotConnectParams::Tcp for replication connections.
pub fn to_slot_connect_params(params: &ConnParams) -> SlotConnectParams {
    SlotConnectParams::Tcp {
        host: params.host.clone(),
        port: params.port,
        user: params.user.clone(),
        password: params.password.clone(),
        dbname: params.dbname.clone(),
        sslmode: params.sslmode.clone(),
    }
}

/// Whether the given sslmode string requires TLS.
///
/// Note: `sslmode=prefer` is treated as `require` (TLS only, no plaintext
/// fallback). This is stricter than libpq's behavior but avoids accidental
/// plaintext connections. Use `sslmode=allow` or omit sslmode for plaintext.
pub fn sslmode_needs_tls(sslmode: &Option<String>) -> bool {
    match sslmode.as_deref() {
        Some("require") | Some("verify-ca") | Some("verify-full") | Some("prefer") => true,
        _ => false,
    }
}

/// Create a rustls `ClientConfig` with Mozilla root certificates for TLS connections.
pub fn make_rustls_config() -> rustls::ClientConfig {
    // Explicitly install the ring crypto provider — required when both ring and
    // aws-lc-rs features are pulled in by transitive dependencies.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

/// Connect to PostgreSQL with automatic TLS negotiation.
///
/// Detects `sslmode` in the connection string — if it requires TLS, uses
/// `tokio-postgres-rustls` with Mozilla root certificates. Otherwise uses NoTls.
///
/// Accepts both libpq key=value and PostgreSQL URI formats (tokio-postgres
/// handles both natively).
///
/// Returns `(Client, JoinHandle)` — the join handle drives the connection
/// background task and should be kept alive as long as the client is in use.
pub async fn pg_connect(
    connstr: &str,
) -> Result<
    (
        tokio_postgres::Client,
        tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    ),
    String,
> {
    let params = parse_connstr(connstr);
    if sslmode_needs_tls(&params.sslmode) {
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(make_rustls_config());
        let (client, connection) = tokio_postgres::connect(connstr, tls)
            .await
            .map_err(|e| format!("pg connect (tls): {}", e))?;
        let handle = tokio::spawn(connection);
        Ok((client, handle))
    } else {
        let (client, connection) = tokio_postgres::connect(connstr, tokio_postgres::NoTls)
            .await
            .map_err(|e| format!("pg connect: {}", e))?;
        let handle = tokio::spawn(connection);
        Ok((client, handle))
    }
}

/// Build a `pgwire_replication::TlsConfig` from an sslmode string.
pub fn make_pgwire_tls_config(sslmode: &Option<String>) -> pgwire_replication::TlsConfig {
    match sslmode.as_deref() {
        Some("require") => pgwire_replication::TlsConfig::require(),
        Some("verify-ca") => pgwire_replication::TlsConfig::verify_ca(None),
        Some("verify-full") => pgwire_replication::TlsConfig::verify_full(None),
        Some("prefer") => pgwire_replication::TlsConfig::require(),
        _ => pgwire_replication::TlsConfig::disabled(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_connstr: key=value format ---

    #[test]
    fn parse_simple_keyvalue() {
        let p = parse_connstr("host=db.example.com port=5433 user=alice dbname=mydb");
        assert_eq!(p.host, "db.example.com");
        assert_eq!(p.port, 5433);
        assert_eq!(p.user, "alice");
        assert_eq!(p.dbname, "mydb");
        assert!(p.password.is_empty());
        assert!(p.sslmode.is_none());
    }

    #[test]
    fn parse_keyvalue_with_sslmode() {
        let p = parse_connstr("host=db port=5432 sslmode=require user=bob dbname=prod");
        assert_eq!(p.sslmode, Some("require".to_string()));
    }

    #[test]
    fn parse_keyvalue_quoted_password() {
        let p = parse_connstr("host=db user=alice password='my pass' dbname=mydb");
        assert_eq!(p.password, "my pass");
    }

    #[test]
    fn parse_keyvalue_quoted_password_with_quotes() {
        // libpq uses backslash escaping inside single quotes: \' for a literal quote
        let p = parse_connstr(r"host=db user=alice password='it\'s a test' dbname=mydb");
        assert_eq!(p.password, "it's a test");
    }

    #[test]
    fn parse_keyvalue_quoted_password_with_backslash() {
        let p = parse_connstr(r"host=db user=alice password='back\\slash' dbname=mydb");
        assert_eq!(p.password, "back\\slash");
    }

    #[test]
    fn parse_keyvalue_defaults() {
        let p = parse_connstr("");
        assert_eq!(p.host, "localhost");
        assert_eq!(p.port, 5432);
        assert_eq!(p.dbname, "postgres");
    }

    // --- parse_connstr: URI format ---

    #[test]
    fn parse_simple_uri() {
        let p = parse_connstr("postgresql://alice:secret@db.example.com:5433/mydb");
        assert_eq!(p.host, "db.example.com");
        assert_eq!(p.port, 5433);
        assert_eq!(p.user, "alice");
        assert_eq!(p.password, "secret");
        assert_eq!(p.dbname, "mydb");
    }

    #[test]
    fn parse_uri_with_sslmode() {
        let p = parse_connstr("postgresql://alice@db/mydb?sslmode=verify-full");
        assert_eq!(p.sslmode, Some("verify-full".to_string()));
    }

    #[test]
    fn parse_uri_percent_encoded_password() {
        let p = parse_connstr("postgresql://alice:p%40ss%3Dword@db/mydb");
        assert_eq!(p.password, "p@ss=word");
    }

    #[test]
    fn parse_uri_postgres_scheme() {
        let p = parse_connstr("postgres://bob@localhost/testdb");
        assert_eq!(p.user, "bob");
        assert_eq!(p.dbname, "testdb");
    }

    // --- url_decode: UTF-8 multi-byte ---

    #[test]
    fn url_decode_ascii() {
        assert_eq!(url_decode("hello%20world"), "hello world");
    }

    #[test]
    fn url_decode_multibyte_utf8() {
        // é = U+00E9 = UTF-8 bytes C3 A9
        assert_eq!(url_decode("%C3%A9"), "é");
    }

    #[test]
    fn url_decode_no_encoding() {
        assert_eq!(url_decode("plain"), "plain");
    }

    // --- build_tokio_pg_connstr: value quoting ---

    #[test]
    fn build_connstr_simple() {
        let p = ConnParams {
            host: "localhost".into(),
            port: 5432,
            user: "alice".into(),
            password: String::new(),
            dbname: "mydb".into(),
            sslmode: None,
        };
        let s = build_tokio_pg_connstr(&p);
        assert_eq!(s, "host=localhost port=5432 user=alice dbname=mydb");
    }

    #[test]
    fn build_connstr_password_with_spaces() {
        let p = ConnParams {
            host: "localhost".into(),
            port: 5432,
            user: "alice".into(),
            password: "my password".into(),
            dbname: "mydb".into(),
            sslmode: None,
        };
        let s = build_tokio_pg_connstr(&p);
        assert!(s.contains("password='my password'"));
    }

    #[test]
    fn build_connstr_password_with_quotes() {
        let p = ConnParams {
            host: "localhost".into(),
            port: 5432,
            user: "alice".into(),
            password: "it's".into(),
            dbname: "mydb".into(),
            sslmode: None,
        };
        let s = build_tokio_pg_connstr(&p);
        assert!(s.contains(r"password='it\'s'"));
    }

    // --- sslmode_needs_tls ---

    #[test]
    fn sslmode_require_needs_tls() {
        assert!(sslmode_needs_tls(&Some("require".into())));
        assert!(sslmode_needs_tls(&Some("verify-full".into())));
        assert!(sslmode_needs_tls(&Some("prefer".into())));
    }

    #[test]
    fn sslmode_disable_no_tls() {
        assert!(!sslmode_needs_tls(&None));
        assert!(!sslmode_needs_tls(&Some("disable".into())));
        assert!(!sslmode_needs_tls(&Some("allow".into())));
    }

    // --- round-trip: parse → build → re-parse ---

    #[test]
    fn roundtrip_keyvalue() {
        let original = "host=db.example.com port=5433 user=alice dbname=mydb sslmode=require";
        let p1 = parse_connstr(original);
        let rebuilt = build_tokio_pg_connstr(&p1);
        let p2 = parse_connstr(&rebuilt);
        assert_eq!(p1.host, p2.host);
        assert_eq!(p1.port, p2.port);
        assert_eq!(p1.user, p2.user);
        assert_eq!(p1.dbname, p2.dbname);
        assert_eq!(p1.sslmode, p2.sslmode);
    }

    #[test]
    fn roundtrip_quoted_password() {
        let original = "host=db user=alice password='s3cr3t w1th sp@ces' dbname=mydb";
        let p1 = parse_connstr(original);
        assert_eq!(p1.password, "s3cr3t w1th sp@ces");
        let rebuilt = build_tokio_pg_connstr(&p1);
        let p2 = parse_connstr(&rebuilt);
        assert_eq!(p1.password, p2.password);
    }
}
