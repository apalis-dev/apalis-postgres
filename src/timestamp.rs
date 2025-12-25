use apalis_sql::SqlTimestamp;
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::{Database, Postgres};
use std::fmt;

// ============================================================================
// Chrono implementation
// ============================================================================

#[cfg(all(feature = "chrono", not(feature = "time")))]
pub use chrono_impl::*;

#[cfg(all(feature = "chrono", not(feature = "time")))]
mod chrono_impl {
    use super::*;
    use chrono::{DateTime, Utc};

    /// The raw datetime type used by sqlx (for use in query structs)
    pub type RawDateTime = DateTime<Utc>;

    /// Newtype wrapper around `chrono::DateTime<Utc>` for use with apalis-sql.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct PgDateTime(pub DateTime<Utc>);

    impl PgDateTime {
        /// Get the current time
        pub fn now() -> Self {
            Self(Utc::now())
        }

        /// Create from a unix timestamp (seconds since epoch)
        pub fn from_timestamp(secs: i64, nanos: u32) -> Option<Self> {
            DateTime::from_timestamp(secs, nanos).map(Self)
        }
    }

    /// Convert unix timestamp to raw datetime for sqlx queries
    pub fn timestamp_from_unix(secs: i64) -> RawDateTime {
        DateTime::from_timestamp(secs, 0).unwrap_or_else(Utc::now)
    }

    /// Get current time as raw datetime for sqlx queries
    pub fn now_raw() -> RawDateTime {
        Utc::now()
    }

    impl SqlTimestamp for PgDateTime {
        fn to_unix_timestamp(&self) -> i64 {
            self.0.timestamp()
        }
    }

    impl From<DateTime<Utc>> for PgDateTime {
        fn from(dt: DateTime<Utc>) -> Self {
            Self(dt)
        }
    }

    impl std::ops::Deref for PgDateTime {
        type Target = DateTime<Utc>;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl fmt::Display for PgDateTime {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt(f)
        }
    }

    // sqlx trait implementations - delegate to inner type
    impl sqlx::Type<Postgres> for PgDateTime {
        fn type_info() -> <Postgres as Database>::TypeInfo {
            <DateTime<Utc> as sqlx::Type<Postgres>>::type_info()
        }

        fn compatible(ty: &<Postgres as Database>::TypeInfo) -> bool {
            <DateTime<Utc> as sqlx::Type<Postgres>>::compatible(ty)
        }
    }

    impl<'r> sqlx::Decode<'r, Postgres> for PgDateTime {
        fn decode(
            value: sqlx::postgres::PgValueRef<'r>,
        ) -> Result<Self, BoxDynError> {
            let dt = <DateTime<Utc> as sqlx::Decode<'r, Postgres>>::decode(value)?;
            Ok(Self(dt))
        }
    }

    impl sqlx::Encode<'_, Postgres> for PgDateTime {
        fn encode_by_ref(
            &self,
            buf: &mut sqlx::postgres::PgArgumentBuffer,
        ) -> Result<IsNull, BoxDynError> {
            <DateTime<Utc> as sqlx::Encode<'_, Postgres>>::encode_by_ref(&self.0, buf)
        }
    }
}

// ============================================================================
// Time implementation
// ============================================================================

#[cfg(feature = "time")]
pub use time_impl::*;

#[cfg(feature = "time")]
mod time_impl {
    use super::*;
    use time::OffsetDateTime;

    /// The raw datetime type used by sqlx (for use in query structs)
    pub type RawDateTime = OffsetDateTime;

    /// Newtype wrapper around `time::OffsetDateTime` for use with apalis-sql.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct PgDateTime(pub OffsetDateTime);

    impl PgDateTime {
        /// Get the current time
        pub fn now() -> Self {
            Self(OffsetDateTime::now_utc())
        }

        /// Create from a unix timestamp (seconds since epoch)
        pub fn from_timestamp(secs: i64, nanos: u32) -> Option<Self> {
            OffsetDateTime::from_unix_timestamp_nanos(secs as i128 * 1_000_000_000 + nanos as i128)
                .ok()
                .map(Self)
        }
    }

    /// Convert unix timestamp to raw datetime for sqlx queries
    pub fn timestamp_from_unix(secs: i64) -> RawDateTime {
        OffsetDateTime::from_unix_timestamp(secs).unwrap_or_else(|_| OffsetDateTime::now_utc())
    }

    /// Get current time as raw datetime for sqlx queries
    pub fn now_raw() -> RawDateTime {
        OffsetDateTime::now_utc()
    }

    impl SqlTimestamp for PgDateTime {
        fn to_unix_timestamp(&self) -> i64 {
            self.0.unix_timestamp()
        }
    }

    impl From<OffsetDateTime> for PgDateTime {
        fn from(dt: OffsetDateTime) -> Self {
            Self(dt)
        }
    }

    impl std::ops::Deref for PgDateTime {
        type Target = OffsetDateTime;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl fmt::Display for PgDateTime {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt(f)
        }
    }

    // sqlx trait implementations - delegate to inner type
    impl sqlx::Type<Postgres> for PgDateTime {
        fn type_info() -> <Postgres as Database>::TypeInfo {
            <OffsetDateTime as sqlx::Type<Postgres>>::type_info()
        }

        fn compatible(ty: &<Postgres as Database>::TypeInfo) -> bool {
            <OffsetDateTime as sqlx::Type<Postgres>>::compatible(ty)
        }
    }

    impl<'r> sqlx::Decode<'r, Postgres> for PgDateTime {
        fn decode(
            value: sqlx::postgres::PgValueRef<'r>,
        ) -> Result<Self, BoxDynError> {
            let dt = <OffsetDateTime as sqlx::Decode<'r, Postgres>>::decode(value)?;
            Ok(Self(dt))
        }
    }

    impl sqlx::Encode<'_, Postgres> for PgDateTime {
        fn encode_by_ref(
            &self,
            buf: &mut sqlx::postgres::PgArgumentBuffer,
        ) -> Result<IsNull, BoxDynError> {
            <OffsetDateTime as sqlx::Encode<'_, Postgres>>::encode_by_ref(&self.0, buf)
        }
    }
}

#[cfg(not(any(feature = "chrono", feature = "time")))]
compile_error!("Either 'chrono' or 'time' feature must be enabled");
