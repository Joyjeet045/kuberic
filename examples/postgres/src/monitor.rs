use crate::instance::PgError;

pub use crate::native::PgNativeObserver as PgMonitor;

/// Parse the PostgreSQL high32/low32 WAL byte offset into the v2 LSN domain.
pub fn parse_pg_lsn(value: &str) -> Result<i64, PgError> {
    let (high, low) = value
        .split_once('/')
        .ok_or_else(|| PgError::Query(format!("invalid LSN format: {value}")))?;
    let high = u32::from_str_radix(high, 16)
        .map_err(|error| PgError::Query(format!("LSN high: {error}")))?;
    let low = u32::from_str_radix(low, 16)
        .map_err(|error| PgError::Query(format!("LSN low: {error}")))?;
    i64::try_from((u64::from(high) << 32) | u64::from(low))
        .map_err(|_| PgError::Query("LSN exceeds the v2 domain".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_native_lsn_without_truncation() {
        for (text, expected) in [
            ("0/0", 0),
            ("0/1", 1),
            ("0/16B3748", 0x16B3748),
            ("1/0", 0x1_0000_0000),
            ("FF/FFFFFFFF", 0xFF_FFFF_FFFF),
        ] {
            assert_eq!(parse_pg_lsn(text).unwrap(), expected);
        }
        for invalid in ["", "invalid", "0/0/0", "0/100000000", "80000000/0"] {
            assert!(parse_pg_lsn(invalid).is_err());
        }
    }
}
