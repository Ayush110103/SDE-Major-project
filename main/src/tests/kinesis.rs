// Copyright (c) 2020-present, UMD Database Group.
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Kinesis events for testing.

use crate::tests::DataRecord;
use base64::{decode, encode};
use chrono::{DateTime, TimeZone, Utc};
use fake::{Dummy, Fake, Faker};
use rand::Rng;
use serde::de::{Deserializer, Error as DeError};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

#[derive(Dummy, Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct KinesisEvent {
    #[serde(rename = "Records")]
    #[dummy(faker = "(Faker, 1000)")]
    pub records: Vec<KinesisEventRecord>,
}

#[derive(Dummy, Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct KinesisEventRecord {
    #[serde(rename = "kinesis")]
    pub kinesis: KinesisRecord,
    #[serde(rename = "eventSource")]
    pub event_source: String,
    #[serde(rename = "eventVersion")]
    pub event_version: String,
}

#[derive(Dummy, Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct KinesisRecord {
    #[serde(rename = "approximateArrivalTimestamp")]
    pub approximate_arrival_timestamp: i64,
    #[serde(rename = "data")]
    pub data: String,
}

fn deserialize_base64<'de, D>(deserializer: D) -> core::result::Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = String::deserialize(deserializer)?;
    decode(&s).map_err(DeError::custom)
}

fn serialize_base64<S>(value: &[u8], serializer: S) -> core::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&encode(value))
}

fn serialize_seconds<S>(
    date: &DateTime<Utc>,
    serializer: S,
) -> core::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let seconds = date.timestamp();
    serializer.serialize_f64(seconds as f64)
}

fn deserialize_seconds<'de, D>(deserializer: D) -> core::result::Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let timestamp: f64 = Deserialize::deserialize(deserializer)?;
    let seconds = timestamp as i64;
    let nanos = ((timestamp - seconds as f64) * 1e9) as u32;
    Utc.timestamp_opt(seconds, nanos).single().ok_or_else(|| {
        DeError::custom("Invalid timestamp")
    })
}
