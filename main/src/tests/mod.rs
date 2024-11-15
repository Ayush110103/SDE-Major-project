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

use crate::datasource::DataSource;
use crate::runtime::plan;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;
use datafusion::physical_plan::ExecutionPlan;
use serde_json::Value;
use std::sync::Arc;

pub fn random_batches(
    rows: usize,
    batch_nums: usize,
    partition_nums: usize,
) -> Vec<Vec<RecordBatch>> {
    (0..partition_nums)
        .map(|_| {
            (0..batch_nums)
                .map(|_| {
                    RecordBatch::try_new(
                        Arc::new(Schema::new(vec![
                            Field::new("c1", DataType::Int64, false),
                            Field::new("c2", DataType::Int64, false),
                            Field::new("c3", DataType::Utf8, false),
                        ])),
                        vec![
                            Arc::new(Int64Array::from(vec![1; rows])),
                            Arc::new(Int64Array::from(vec![2; rows])),
                            Arc::new(StringArray::from(vec!["data"; rows])),
                        ],
                    )
                    .unwrap()
                })
                .collect()
        })
        .collect()
}

pub fn register_table(schema: &SchemaRef, table_name: &str) -> ExecutionContext {
    let mut ctx = ExecutionContext::new();
    let batch = RecordBatch::new_empty(schema.clone());
    let table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table(table_name, Arc::new(table)).unwrap();
    ctx
}
