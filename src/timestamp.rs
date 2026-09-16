use serde::{Deserialize, Serialize};
use sqlx::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef, Postgres, types::Oid},
    types::Type,
};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Timestamp(pub u64);

const POSTGRES_EPOCH: i64 = 946_684_800;

impl Type<Postgres> for Timestamp {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_oid(Oid(1184))
    }
}

impl PgHasArrayType for Timestamp {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_oid(Oid(1185))
    }
}

impl<'q> Encode<'q, Postgres> for Timestamp {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        // PostgreSQL stores TIMESTAMPTZ as microseconds
        // since 2000-01-01 00:00:00 UTC.
        let postgres_micros = (self.0 as i64 - POSTGRES_EPOCH)
            .checked_mul(1_000_000)
            .ok_or("timestamp overflow")?;

        <i64 as Encode<Postgres>>::encode(postgres_micros, buf)
    }

    fn size_hint(&self) -> usize {
        8
    }
}

impl<'r> Decode<'r, Postgres> for Timestamp {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        // PostgreSQL TIMESTAMPTZ -> microseconds since 2000.
        let postgres_micros = <i64 as Decode<Postgres>>::decode(value)?;

        // Convert to Unix seconds.
        let unix_seconds = postgres_micros / 1_000_000 + POSTGRES_EPOCH;

        if unix_seconds < 0 {
            return Err("timestamp is before Unix epoch".into());
        }

        Ok(Timestamp(unix_seconds as u64))
    }
}

impl From<i64> for Timestamp {
    fn from(num: i64) -> Self {
        Self(num as u64)
    }
}

impl Timestamp {
    pub fn now() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("System time is before Unix epoch");
        Timestamp(now.as_secs())
    }
}
