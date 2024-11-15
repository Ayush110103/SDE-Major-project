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

//! Flock error types

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;
use std::error;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;
use sqlparser::parser::ParserError;

/// Result type for operations that could result in an [FlockError]
pub type Result<T> = result::Result<T, FlockError>;

/// Flock error
#[derive(Debug)]
pub enum FlockError {
    LambdaError(Box<dyn std::error::Error + Send + Sync>),
    IoError(io::Error),
    Parquet(ParquetError),
    SQL(ParserError),
    Arrow(ArrowError),
    DataFusion(DataFusionError),
    Internal(String),
    NotImplemented(String),
    Execution(String),
    AWS(String),
}

impl From<io::Error> for FlockError {
    fn from(e: io::Error) -> Self {
        FlockError::IoError(e)
    }
}

impl From<ParserError> for FlockError {
    fn from(e: ParserError) -> Self {
        FlockError::SQL(e)
    }
}

impl From<DataFusionError> for FlockError {
    fn from(e: DataFusionError) -> Self {
        FlockError::DataFusion(e)
    }
}

impl From<ArrowError> for FlockError {
    fn from(e: ArrowError) -> Self {
        FlockError::Arrow(e)
    }
}

impl From<ParquetError> for FlockError {
    fn from(e: ParquetError) -> Self {
        FlockError::Parquet(e)
    }
}

impl From<&str> for FlockError {
    fn from(e: &str) -> Self {
        FlockError::Internal(e.to_string())
    }
}

impl Display for FlockError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FlockError::LambdaError(desc) => write!(f, "Lambda error: {}", desc),
            FlockError::IoError(desc) => write!(f, "IO error: {}", desc),
            FlockError::Parquet(desc) => write!(f, "Parquet error: {}", desc),
            FlockError::SQL(desc) => write!(f, "SQL error: {}", desc),
            FlockError::Arrow(desc) => write!(f, "Arrow error: {}", desc),
            FlockError::DataFusion(desc) => write!(f, "DataFusion error: {}", desc),
            FlockError::Internal(desc) => write!(f, "Internal error: {}", desc),
            FlockError::NotImplemented(desc) => write!(f, "Not implemented: {}", desc),
            FlockError::Execution(desc) => write!(f, "Execution error: {}", desc),
            FlockError::AWS(desc) => write!(f, "AWS error: {}", desc),
        }
    }
}

impl error::Error for FlockError {}
