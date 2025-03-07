use anyhow::{Result, Context};
use clap::{Parser, Subcommand};
use futures_util::stream::{FuturesUnordered, StreamExt};
use ini::Ini;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sqlx::{Pool, Row, any::AnyRow, Column};
use std::collections::{HashMap, HashSet};
use std::fs;
use tokio::fs::File as TokioFile;
use tokio::io::{BufWriter, AsyncWriteExt};
use async_compression::tokio::write::GzipEncoder;
use sysinfo::System;
use tracing::{info, warn, error};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::filter::EnvFilter;

/// CLI arguments using clap.
#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Dump the source database to SQL file(s).
    Dump {
        /// Shape file name (without the .shape.json extension)
        name: String,
    },
    /// Restore the source database from dump file(s).
    Restore {
        /// Shape file name (without the .shape.json extension)
        name: String,
    },
    /// Run migration according to the shape file.
    /// If mappings exist, performs migration; otherwise, performs a dump.
    Run {
        /// Shape file name (without the .shape.json extension)
        name: String,
    },
}

/// Holds directories for shapes, dumps and logs from config.ini.
struct ConfigPaths {
    shapes: String,
    dumps: String,
    logs: String,
}

impl ConfigPaths {
    fn load_from_file(path: &str) -> Result<Self> {
        let conf = Ini::load_from_file(path)
            .context("Failed to load config.ini")?;
        // Use the general (default) section.
        let section = conf.general_section();
        let shapes = section.get::<&str>("shapes").unwrap_or("shapes").to_string();
        let dumps = section.get::<&str>("dumps").unwrap_or("dumps").to_string();
        let logs = section.get::<&str>("logs").unwrap_or("logs").to_string();
        Ok(ConfigPaths { shapes, dumps, logs })
    }
}

/// Initializes logging with two layers: one writing to stdout and one writing to a daily rotated log file.
fn init_logging(logs_dir: &str) -> tracing_appender::non_blocking::WorkerGuard {
    use tracing_appender::rolling::daily;
    let file_appender = daily(logs_dir, "application.log");
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    let console_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stdout);
    let file_layer = tracing_subscriber::fmt::layer().with_writer(file_writer);
    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .with(EnvFilter::from_default_env())
        .init();
    guard
}

/// Shape file format.
/// New properties:
/// - compress: if true, dump files are gzipped.
/// - split_dump: if true, each table is dumped into its own SQL file.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ConfigShape {
    source: SourceConfig,
    /// Optional mappings for migration. If missing or empty, dump/restore operations are performed.
    mappings: Option<Vec<Mapping>>,
    #[serde(default)]
    compress: bool,
    #[serde(default)]
    split_dump: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SourceConfig {
    connection: String,
    tables: Vec<TableConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TableConfig {
    name: String,
    #[serde(default, rename = "where")]
    where_clause: Option<HashMap<String, JsonValue>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Mapping {
    connection: String,
    tables: Vec<MappingTable>,
}

/// Column mapping is defined as an array in the shape file:
/// e.g. ["id", "user_id"] or ["created_at", "created", ["normalize_timestamp", "trim"]]
#[derive(Debug, Serialize, Clone, PartialEq)]
struct ColumnMapping {
    source: String,
    target: String,
    transformers: Vec<String>,
}

impl<'de> Deserialize<'de> for ColumnMapping {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = serde_json::Value::deserialize(deserializer)?;
        if let serde_json::Value::Array(arr) = v {
            if arr.len() < 2 {
                return Err(serde::de::Error::custom("ColumnMapping array must have at least two elements"));
            }
            let source = arr.get(0)
                .and_then(|v| v.as_str())
                .ok_or_else(|| serde::de::Error::custom("source must be a string"))?
                .to_string();
            let target = arr.get(1)
                .and_then(|v| v.as_str())
                .ok_or_else(|| serde::de::Error::custom("target must be a string"))?
                .to_string();
            let transformers = if arr.len() > 2 {
                let third = &arr[2];
                if third.is_string() {
                    vec![third.as_str().unwrap().to_string()]
                } else if let serde_json::Value::Array(transformer_arr) = third {
                    transformer_arr.iter()
                        .map(|t| {
                            t.as_str()
                                .map(|s| s.to_string())
                                .ok_or_else(|| serde::de::Error::custom("each transformer must be a string"))
                        })
                        .collect::<Result<Vec<String>, D::Error>>()?
                } else {
                    return Err(serde::de::Error::custom("Invalid transformer format"));
                }
            } else {
                vec![]
            };
            Ok(ColumnMapping { source, target, transformers })
        } else {
            Err(serde::de::Error::custom("ColumnMapping must be an array"))
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct MappingTable {
    source_table: String,
    target_table: String,
    #[serde(default)]
    columns: Vec<ColumnMapping>,
}

type RowData = HashMap<String, JsonValue>;

#[derive(Debug, Clone, PartialEq)]
enum DbType {
    MySql,
    Postgres,
}

impl DbType {
    fn from_connection_string(s: &str) -> Result<Self> {
        if s.starts_with("mysql://") {
            Ok(DbType::MySql)
        } else if s.starts_with("postgres://") || s.starts_with("postgresql://") {
            Ok(DbType::Postgres)
        } else {
            Err(anyhow::anyhow!("Unknown database type in connection string: {}", s))
        }
    }
}

#[derive(Clone)]
enum TransformerType {
    NormalizeTimestamp,
    ToLowercase,
    Trim,
    Round,
}

impl TransformerType {
    fn transform(&self, value: JsonValue) -> Result<JsonValue> {
        match self {
            TransformerType::NormalizeTimestamp => {
                if let JsonValue::String(ts) = value {
                    Ok(JsonValue::String(format!("normalized_{}", ts)))
                } else {
                    Err(anyhow::anyhow!("Expected string for timestamp, got {:?}", value))
                }
            }
            TransformerType::ToLowercase => {
                if let JsonValue::String(s) = value {
                    Ok(JsonValue::String(s.to_lowercase()))
                } else {
                    Err(anyhow::anyhow!("Expected string, got {:?}", value))
                }
            }
            TransformerType::Trim => {
                if let JsonValue::String(s) = value {
                    Ok(JsonValue::String(s.trim().to_string()))
                } else {
                    Err(anyhow::anyhow!("Expected string, got {:?}", value))
                }
            }
            TransformerType::Round => {
                if let JsonValue::Number(ref n) = value {
                    if let Some(f) = n.as_f64() {
                        Ok(JsonValue::Number(
                            serde_json::Number::from_f64(f.round()).unwrap_or_else(|| n.clone())
                        ))
                    } else {
                        Ok(value)
                    }
                } else {
                    Err(anyhow::anyhow!("Expected number, got {:?}", value))
                }
            }
        }
    }
}

enum Writer {
    Plain(BufWriter<TokioFile>),
    Compressed(GzipEncoder<BufWriter<TokioFile>>),
}

/// Migrator for mapping operations.
struct Migrator {
    config: ConfigShape,
    transformers: HashMap<String, TransformerType>,
    source_pool: Pool<sqlx::Any>,
    source_type: DbType,
    target_pools: HashMap<String, (DbType, Pool<sqlx::Any>)>,
}

impl Migrator {
    async fn new(config: ConfigShape) -> Result<Self> {
        let transformers = HashMap::from([
            ("normalize_timestamp".to_string(), TransformerType::NormalizeTimestamp),
            ("to_lowercase".to_string(), TransformerType::ToLowercase),
            ("trim".to_string(), TransformerType::Trim),
            ("round".to_string(), TransformerType::Round),
        ]);

        let source_type = DbType::from_connection_string(&config.source.connection)?;
        let source_pool = sqlx::any::AnyPoolOptions::new()
            .connect(&config.source.connection)
            .await
            .context("Failed to connect to source database")?;

        let mut target_pools = HashMap::new();
        if let Some(mappings) = &config.mappings {
            let mut unique_connections = HashSet::new();
            for mapping in mappings {
                if unique_connections.insert(mapping.connection.clone()) {
                    let db_type = DbType::from_connection_string(&mapping.connection)?;
                    let pool = sqlx::any::AnyPoolOptions::new()
                        .connect(&mapping.connection)
                        .await
                        .context(format!("Failed to connect to target '{}'", mapping.connection))?;
                    target_pools.insert(mapping.connection.clone(), (db_type, pool));
                }
            }
        }

        let migrator = Self {
            config,
            transformers,
            source_pool,
            source_type,
            target_pools,
        };

        if let Some(mappings) = &migrator.config.mappings {
            for mapping in mappings {
                if let Some((_, pool)) = migrator.target_pools.get(&mapping.connection) {
                    for table in &mapping.tables {
                        migrator.validate_target_schema(pool, table).await?;
                    }
                } else {
                    warn!("Target pool not initialized for '{}'", mapping.connection);
                }
            }
        }

        Ok(migrator)
    }

    async fn validate_target_schema(&self, pool: &Pool<sqlx::Any>, mapping: &MappingTable) -> Result<()> {
        let target_type = if let Some(mappings) = &self.config.mappings {
            DbType::from_connection_string(&mappings[0].connection)?
        } else {
            self.source_type.clone()
        };
        let query = match target_type {
            DbType::MySql => format!(
                "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'",
                mapping.target_table
            ),
            DbType::Postgres => format!(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}'",
                mapping.target_table
            ),
        };
        let rows = sqlx::query(&query).fetch_all(pool).await?;
        let target_schema: HashMap<String, String> = rows.into_iter()
            .map(|r| (r.get::<String, _>("column_name"), r.get::<String, _>("data_type").to_lowercase()))
            .collect();
        for col in &mapping.columns {
            let inferred = self.infer_type(&mapping.source_table, &col.source, None, Some(&target_type));
            if let Some(target_type_str) = target_schema.get(&col.target) {
                if !Self::types_compatible(&inferred, target_type_str) {
                    warn!(
                        "Type mismatch for {}.{}: inferred {}, target requires {}",
                        mapping.target_table, col.target, inferred, target_type_str
                    );
                }
            }
        }
        Ok(())
    }

    fn types_compatible(inferred: &str, target: &str) -> bool {
        matches!(
            (inferred, target),
            ("INTEGER", "BIGINT") | ("BIGINT", "INTEGER") | ("TEXT", "VARCHAR") |
            ("TIMESTAMP", "TIMESTAMP") | ("TIMESTAMP", "DATETIME") | ("DATETIME", "TIMESTAMP") | ("JSON", "JSON")
        )
    }

    fn calculate_chunk_size(_chunk_size: Option<&JsonValue>) -> usize {
        let mut system = System::new_all();
        system.refresh_memory();
        let estimated_row_size = 1024;
        let target_chunk_memory = 1 * 1024 * 1024;
        let chunk_size = (target_chunk_memory / estimated_row_size) as usize;
        chunk_size.clamp(100, 10_000)
    }

    fn build_select_query(&self, table: &TableConfig) -> (String, Vec<String>) {
        let mut query = format!("SELECT * FROM {}", table.name);
        let mut params = vec![];
        if let Some(where_clause) = &table.where_clause {
            let mut conditions: Vec<String> = Vec::new();
            for (key, value) in where_clause {
                let column = key.as_str();
                match value {
                    JsonValue::Array(arr) if !arr.is_empty() => {
                        let op = arr[0].as_str().unwrap_or("=").to_uppercase();
                        match op.as_str() {
                            "IN" | "NOT IN" => {
                                if arr.len() == 2 && arr[1].is_array() {
                                    let values = arr[1].as_array().unwrap().iter()
                                        .map(|v| serde_json::to_string(v).unwrap_or("NULL".to_string()))
                                        .collect::<Vec<_>>();
                                    let placeholders = values.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
                                    conditions.push(format!("{} {} ({})", column, op, placeholders));
                                    params.extend(values);
                                }
                            }
                            "BETWEEN" | "NOT BETWEEN" => {
                                if arr.len() == 2 && arr[1].is_array() && arr[1].as_array().unwrap().len() == 2 {
                                    conditions.push(format!("{} {} ? AND ?", column, op));
                                    params.push(serde_json::to_string(&arr[1][0]).unwrap_or("NULL".to_string()));
                                    params.push(serde_json::to_string(&arr[1][1]).unwrap_or("NULL".to_string()));
                                }
                            }
                            "IS NULL" | "IS NOT NULL" => {
                                if arr.len() == 1 {
                                    conditions.push(format!("{} {}", column, op));
                                }
                            }
                            "=" | "!=" | "<>" | ">" | "<" | ">=" | "<=" | "LIKE" | "NOT LIKE" => {
                                if arr.len() == 2 {
                                    conditions.push(format!("{} {} ?", column, op));
                                    params.push(serde_json::to_string(&arr[1]).unwrap_or("NULL".to_string()));
                                }
                            }
                            _ => warn!("Unsupported operator {}", op),
                        }
                    }
                    _ => {
                        conditions.push(format!("{} = ?", column));
                        params.push(serde_json::to_string(value).unwrap_or("NULL".to_string()));
                    }
                }
            }
            if !conditions.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&conditions.join(" AND "));
            }
        }
        (query, params)
    }

    /// Helper: Try to extract a column's value from a row by trying several types.
    fn value_to_json_from_row(row: &AnyRow, col: &str) -> Result<JsonValue> {
        if let Ok(i) = row.try_get::<i64, &str>(col) {
            return Ok(json!(i));
        }
        if let Ok(f) = row.try_get::<f64, &str>(col) {
            return Ok(json!(f));
        }
        if let Ok(s) = row.try_get::<String, &str>(col) {
            return Ok(json!(s));
        }
        if let Ok(b) = row.try_get::<Vec<u8>, &str>(col) {
            if let Ok(s) = std::str::from_utf8(&b) {
                return Ok(json!(s));
            } else {
                return Ok(json!(b));
            }
        }
        Ok(JsonValue::Null)
    }

    /// Applies transformers (or automatic conversion) to each column in a mapping.
    fn transform_row(
        row: &AnyRow,
        mapping: &MappingTable,
        source_type: DbType,
        target_type: DbType,
        transformers: &HashMap<String, TransformerType>
    ) -> Result<RowData> {
        let mut data = RowData::new();
        for col in &mapping.columns {
            let mut value = Self::value_to_json_from_row(row, col.source.as_str())?;
            if !col.transformers.is_empty() {
                for transformer in &col.transformers {
                    value = transformers
                        .get(transformer)
                        .ok_or_else(|| anyhow::anyhow!("Transformer {} not found", transformer))?
                        .transform(value)?;
                }
            } else if source_type != target_type {
                value = auto_transform(value, source_type.clone(), target_type.clone())?;
            }
            data.insert(col.target.clone(), value);
        }
        Ok(data)
    }

    /// Runs migration: if mappings exist, applies migration logic; otherwise, dumps source.
    async fn run(&self) -> Result<()> {
        let (tx, rx) = tokio::sync::mpsc::channel::<(String, String, Vec<RowData>)>(100);

        for table in &self.config.source.tables {
            let tx = tx.clone();
            let source_pool = self.source_pool.clone();
            let source_table_config = table.clone();
            let mappings = if let Some(m) = &self.config.mappings { m.clone() } else { vec![] };
            let transformers = self.transformers.clone();
            let chunk_size = Self::calculate_chunk_size(None);
            let source_type = self.source_type.clone();

            let (query, params) = self.build_select_query(&source_table_config);
            info!("Query for {}: {}", source_table_config.name, query);
            info!("Using chunk_size: {} for table {}", chunk_size, source_table_config.name);

            // If no mapping exists, dump the table's data.
            if mappings.is_empty() {
                let mut rows = sqlx::query(&query).fetch(&source_pool);
                let mut batch = Vec::new();
                while let Some(row) = rows.next().await.transpose()? {
                    let mut row_map = RowData::new();
                    for column in row.columns() {
                        let col_name = column.name();
                        let value = Self::value_to_json_from_row(&row, col_name)?;
                        row_map.insert(col_name.to_string(), value);
                    }
                    batch.push(row_map);
                    if batch.len() >= chunk_size {
                        tx.send((self.config.source.connection.clone(), source_table_config.name.clone(), batch)).await?;
                        batch = Vec::new();
                    }
                }
                if !batch.is_empty() {
                    tx.send((self.config.source.connection.clone(), source_table_config.name.clone(), batch)).await?;
                }
                continue;
            }

            let mapping_configs: Vec<(String, MappingTable)> = {
                let mut v = Vec::new();
                for mapping in &mappings {
                    for table_mapping in &mapping.tables {
                        if table_mapping.source_table == source_table_config.name {
                            v.push((mapping.connection.clone(), table_mapping.clone()));
                        }
                    }
                }
                v
            };

            tokio::spawn(async move {
                let mut q = sqlx::query(&query);
                for param in params {
                    q = q.bind(param);
                }
                let mut rows = q.fetch(&source_pool);
                let mut batches: HashMap<(String, String), Vec<RowData>> = HashMap::new();

                while let Some(row) = rows.next().await.transpose()? {
                    for (target_connection, mapping_table) in &mapping_configs {
                        let key = (target_connection.clone(), mapping_table.target_table.clone());
                        let target_type = DbType::from_connection_string(target_connection)
                            .unwrap_or(source_type.clone());
                        let transformed = Self::transform_row(&row, mapping_table, source_type.clone(), target_type, &transformers)?;
                        let batch = batches.entry(key.clone()).or_default();
                        batch.push(transformed);
                        if batch.len() >= chunk_size {
                            let batch_to_send = batches.remove(&key).unwrap();
                            tx.send((target_connection.clone(), mapping_table.target_table.clone(), batch_to_send)).await?;
                        }
                    }
                }

                for ((target_connection, target_table), batch) in batches.into_iter() {
                    if !batch.is_empty() {
                        tx.send((target_connection, target_table, batch)).await?;
                    }
                }

                Ok::<(), anyhow::Error>(())
            });
        }

        drop(tx);

        if !self.target_pools.is_empty() {
            let mut rx = rx;
            let target_pools = self.target_pools.clone();
            while let Some((target_connection, table, batch)) = rx.recv().await {
                if let Some((db_type, pool)) = target_pools.get(&target_connection) {
                    if let Err(e) = Self::write_batch_to_db(pool, db_type, &table, &batch).await {
                        error!("Failed to write batch to {} (target: {}): {}", table, target_connection, e);
                    } else {
                        info!("Wrote {} rows to {} (target: {})", batch.len(), table, target_connection);
                    }
                } else {
                    error!("No pool found for target '{}'", target_connection);
                }
            }
        } else {
            // Single dump file mode.
            let dump_filename = if self.config.compress {
                format!("{}/{}.sql.gz", "dumps", "dump_all")
            } else {
                format!("{}/{}.sql", "dumps", "dump_all")
            };
            let file = TokioFile::create(&dump_filename).await
                .context(format!("Failed to create dump file {}", dump_filename))?;
            let buffered_writer = BufWriter::with_capacity(4 * 1024 * 1024, file);
            let mut writer = if self.config.compress {
                Writer::Compressed(GzipEncoder::new(buffered_writer))
            } else {
                Writer::Plain(buffered_writer)
            };
            let mut rx = rx;
            while let Some((_, target_table, batch)) = rx.recv().await {
                Self::write_batch_to_file(&mut writer, &target_table, &batch).await?;
                match &mut writer {
                    Writer::Plain(w) => w.flush().await?,
                    Writer::Compressed(w) => w.flush().await?,
                }
                info!("Dumped {} rows for {}", batch.len(), target_table);
            }
        }

        Ok(())
    }

    async fn write_batch_to_db(pool: &Pool<sqlx::Any>, db_type: &DbType, table: &str, batch: &[RowData]) -> Result<()> {
        let mut tx = pool.begin().await?;
        let mut query = format!("INSERT INTO {} (", table);
        let columns: Vec<String> = batch[0].keys().cloned().collect();
        query.push_str(&columns.join(", "));
        query.push_str(") VALUES ");

        let mut values = Vec::new();
        for (i, row) in batch.iter().enumerate() {
            let placeholders: Vec<String> = columns
                .iter()
                .enumerate()
                .map(|(j, _)| format!("${}", i * columns.len() + j + 1))
                .collect();
            values.extend(columns.iter().map(|col| serde_json::to_string(&row[col]).unwrap_or("NULL".to_string())));
            query.push_str(&format!("({}),", placeholders.join(", ")));
        }
        query.truncate(query.len() - 1);

        if matches!(db_type, DbType::MySql) {
            query = query.replace('$', "?");
        }

        let mut q = sqlx::query(&query);
        for value in values {
            q = q.bind(value);
        }
        q.execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn write_batch_to_file(writer: &mut Writer, table: &str, batch: &[RowData]) -> Result<()> {
        let mut query = format!("INSERT INTO {} (", table);
        let columns: Vec<String> = batch[0].keys().cloned().collect();
        query.push_str(&columns.join(", "));
        query.push_str(") VALUES ");
        let mut values = Vec::new();
        for row in batch {
            let row_values: Vec<String> = columns.iter().map(|col| {
                match &row[col] {
                    JsonValue::Number(n) => n.to_string(),
                    JsonValue::String(s) => format!("'{}'", s.replace("'", "''")),
                    JsonValue::Bool(b) => b.to_string(),
                    _ => "NULL".to_string(),
                }
            }).collect();
            values.push(format!("({})", row_values.join(", ")));
        }
        query.push_str(&values.join(", "));
        query.push_str(";\n");
        match writer {
            Writer::Plain(w) => w.write_all(query.as_bytes()).await?,
            Writer::Compressed(w) => w.write_all(query.as_bytes()).await?,
        }
        Ok(())
    }

    fn infer_type(&self, _table: &str, column: &str, user_type: Option<&str>, target_type: Option<&DbType>) -> String {
        let rust_type = match self.source_type {
            DbType::MySql => match column {
                "id" => "i32",
                "created_at" => "timestamp",
                _ => "string",
            },
            DbType::Postgres => match column {
                "id" => "i32",
                "created_at" => "timestamp",
                _ => "string",
            },
        };

        let final_type = user_type.unwrap_or(rust_type);
        if let Some(user) = user_type {
            if user != rust_type {
                let compatible = match (rust_type, user) {
                    ("i32", "i64") | ("i64", "i32") => true,
                    ("string", "timestamp") | ("timestamp", "string") => true,
                    ("f64", "i32") | ("f64", "i64") => true,
                    _ => false,
                };
                if !compatible {
                    warn!(
                        "Type mismatch for {}.{}: detected {}, user specified {}.",
                        _table, column, rust_type, user
                    );
                }
            }
        }
        let target_db_type = target_type.unwrap_or(&self.source_type);
        match target_db_type {
            DbType::MySql => match final_type {
                "i32" => "INT",
                "i64" => "BIGINT",
                "f64" => "DOUBLE",
                "string" => "TEXT",
                "timestamp" => "DATETIME",
                "bytes" => "BLOB",
                "json" => "JSON",
                "bool" => "TINYINT(1)",
                _ => "TEXT",
            }
            .to_string(),
            DbType::Postgres => match final_type {
                "i32" => "INTEGER",
                "i64" => "BIGINT",
                "f64" => "DOUBLE PRECISION",
                "string" => "TEXT",
                "timestamp" => "TIMESTAMP",
                "bytes" => "BYTEA",
                "json" => "JSONB",
                "bool" => "BOOLEAN",
                _ => "TEXT",
            }
            .to_string(),
        }
    }
}

/// Automatically transforms a JSON value when source and target DB differ.
fn auto_transform(value: JsonValue, source_type: DbType, target_type: DbType) -> Result<JsonValue> {
    if source_type == target_type {
        return Ok(value);
    }
    match value {
        JsonValue::String(ref s) => {
            if s.contains('-') && s.contains(':') {
                return Ok(JsonValue::String(format!("converted_{}", s)));
            }
            Ok(value)
        },
        JsonValue::Number(n) => Ok(JsonValue::Number(n)),
        _ => Ok(value),
    }
}

/// Dumps the source database to SQL file(s).
/// If split_dump is true, creates one file per table; otherwise, creates one combined file.
async fn dump_shape(config: &ConfigShape, paths: &ConfigPaths, shape_name: &str) -> Result<()> {
    let pool = sqlx::any::AnyPoolOptions::new()
        .connect(&config.source.connection)
        .await
        .context("Failed to connect to source database")?;
    if config.split_dump {
        let mut tasks = FuturesUnordered::new();
        for table in &config.source.tables {
            let pool = pool.clone();
            let table = table.clone();
            let dumps_dir = paths.dumps.clone();
            let compress = config.compress;
            let shape_name = shape_name.to_string();
            tasks.push(tokio::spawn(async move {
                dump_table(&table, pool, &dumps_dir, &shape_name, compress).await
            }));
        }
        while let Some(res) = tasks.next().await {
            res??;
        }
    } else {
        let chunk_size = 1000;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(String, Vec<RowData>)>(100);
        for table in &config.source.tables {
            let tx = tx.clone();
            let query = format!("SELECT * FROM {}", table.name);
            let pool = pool.clone();
            let table_name = table.name.clone();
            tokio::spawn(async move {
                let mut rows = sqlx::query(&query).fetch(&pool);
                let mut batch = Vec::new();
                while let Some(row) = rows.next().await.transpose()? {
                    let mut row_map = RowData::new();
                    for column in row.columns() {
                        let col_name = column.name();
                        let value = Migrator::value_to_json_from_row(&row, col_name)?;
                        row_map.insert(col_name.to_string(), value);
                    }
                    batch.push(row_map);
                    if batch.len() >= chunk_size {
                        tx.send((table_name.clone(), batch)).await?;
                        batch = Vec::new();
                    }
                }
                if !batch.is_empty() {
                    tx.send((table_name.clone(), batch)).await?;
                }
                Ok::<(), anyhow::Error>(())
            });
        }
        drop(tx);
        let dump_filename = if config.compress {
            format!("{}/{}.sql.gz", paths.dumps, shape_name)
        } else {
            format!("{}/{}.sql", paths.dumps, shape_name)
        };
        let file = TokioFile::create(&dump_filename).await
            .context(format!("Failed to create dump file {}", dump_filename))?;
        let buffered_writer = BufWriter::with_capacity(4 * 1024 * 1024, file);
        let mut writer = if config.compress {
            Writer::Compressed(GzipEncoder::new(buffered_writer))
        } else {
            Writer::Plain(buffered_writer)
        };
        while let Some((table_name, batch)) = rx.recv().await {
            let stmt = generate_insert_statement(&table_name, &batch)?;
            match &mut writer {
                Writer::Plain(w) => w.write_all(stmt.as_bytes()).await?,
                Writer::Compressed(w) => w.write_all(stmt.as_bytes()).await?,
            }
        }
        match &mut writer {
            Writer::Plain(w) => w.flush().await?,
            Writer::Compressed(w) => w.flush().await?,
        }
        info!("Dumped source database to {}", dump_filename);
    }
    Ok(())
}

/// Dumps a single table to its own SQL file.
async fn dump_table(
    table: &TableConfig,
    pool: Pool<sqlx::Any>,
    dumps_dir: &str,
    shape_name: &str,
    compress: bool,
) -> Result<()> {
    let chunk_size = 1000;
    let query = format!("SELECT * FROM {}", table.name);
    let mut rows = sqlx::query(&query).fetch(&pool);
    let mut batch = Vec::new();
    let mut sql_statements = String::new();
    while let Some(row) = rows.next().await.transpose()? {
        let mut row_map = RowData::new();
        for column in row.columns() {
            let col_name = column.name();
            let value = Migrator::value_to_json_from_row(&row, col_name)?;
            row_map.insert(col_name.to_string(), value);
        }
        batch.push(row_map);
        if batch.len() >= chunk_size {
            sql_statements.push_str(&generate_insert_statement(&table.name, &batch)?);
            batch.clear();
        }
    }
    if !batch.is_empty() {
        sql_statements.push_str(&generate_insert_statement(&table.name, &batch)?);
    }
    let dump_filename = if compress {
        format!("{}/{}_{}.sql.gz", dumps_dir, shape_name, table.name)
    } else {
        format!("{}/{}_{}.sql", dumps_dir, shape_name, table.name)
    };
    let file = TokioFile::create(&dump_filename).await
        .context(format!("Failed to create dump file {}", dump_filename))?;
    let mut writer = if compress {
        Writer::Compressed(GzipEncoder::new(BufWriter::new(file)))
    } else {
        Writer::Plain(BufWriter::new(file))
    };
    match &mut writer {
        Writer::Plain(w) => w.write_all(sql_statements.as_bytes()).await?,
        Writer::Compressed(w) => w.write_all(sql_statements.as_bytes()).await?,
    }
    match &mut writer {
        Writer::Plain(w) => w.flush().await?,
        Writer::Compressed(w) => w.flush().await?,
    }
    info!("Dumped table {} to {}", table.name, dump_filename);
    Ok(())
}

/// Generates an INSERT statement from a batch of rows.
fn generate_insert_statement(table: &str, batch: &[RowData]) -> Result<String> {
    if batch.is_empty() {
        return Ok(String::new());
    }
    let columns: Vec<String> = batch[0].keys().cloned().collect();
    let mut stmt = format!("INSERT INTO {} ({}) VALUES ", table, columns.join(", "));
    let mut values = Vec::new();
    for row in batch {
        let row_values: Vec<String> = columns.iter().map(|col| {
            match &row[col] {
                JsonValue::Number(n) => n.to_string(),
                JsonValue::String(s) => format!("'{}'", s.replace("'", "''")),
                JsonValue::Bool(b) => b.to_string(),
                _ => "NULL".to_string(),
            }
        }).collect();
        values.push(format!("({})", row_values.join(", ")));
    }
    stmt.push_str(&values.join(", "));
    stmt.push_str(";\n");
    Ok(stmt)
}

/// Restores the source database from dump file(s).
async fn restore_shape(config: &ConfigShape, paths: &ConfigPaths, shape_name: &str) -> Result<()> {
    if config.split_dump {
        for table in &config.source.tables {
            let dump_filename = if config.compress {
                format!("{}/{}_{}.sql.gz", paths.dumps, shape_name, table.name)
            } else {
                format!("{}/{}_{}.sql", paths.dumps, shape_name, table.name)
            };
            let dump_data = if config.compress {
                fs::read(&dump_filename).context(format!("Failed to read gz dump file {}", dump_filename))?
            } else {
                fs::read(&dump_filename).context(format!("Failed to read dump file {}", dump_filename))?
            };
            let dump_str = String::from_utf8(dump_data).context("Dump file is not valid UTF-8")?;
            let pool = sqlx::any::AnyPoolOptions::new()
                .connect(&config.source.connection)
                .await
                .context("Failed to connect to source database")?;
            for stmt in dump_str.split(';') {
                let stmt = stmt.trim();
                if !stmt.is_empty() {
                    sqlx::query(stmt).execute(&pool).await?;
                }
            }
            info!("Restored table {} from {}", table.name, dump_filename);
        }
    } else {
        let dump_filename = if config.compress {
            format!("{}/{}.sql.gz", paths.dumps, shape_name)
        } else {
            format!("{}/{}.sql", paths.dumps, shape_name)
        };
        let dump_data = if config.compress {
            fs::read(&dump_filename).context("Failed to read gz dump file")?
        } else {
            fs::read(&dump_filename).context("Failed to read dump file")?
        };
        let dump_str = String::from_utf8(dump_data).context("Dump file is not valid UTF-8")?;
        let pool = sqlx::any::AnyPoolOptions::new()
            .connect(&config.source.connection)
            .await
            .context("Failed to connect to source database")?;
        for stmt in dump_str.split(';') {
            let stmt = stmt.trim();
            if !stmt.is_empty() {
                sqlx::query(stmt).execute(&pool).await?;
            }
        }
        info!("Restored source database from {}", dump_filename);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let config_paths = ConfigPaths::load_from_file("config.ini")?;
    let _log_guard = init_logging(&config_paths.logs);

    let cli = Cli::parse();
    let shape_name = match &cli.command {
        Commands::Dump { name } => name,
        Commands::Restore { name } => name,
        Commands::Run { name } => name,
    };
    let shape_file = format!("{}/{}.shape.json", config_paths.shapes, shape_name);
    let shape_content = fs::read_to_string(&shape_file)
        .context(format!("Failed to read shape file {}", shape_file))?;
    let shape: ConfigShape = serde_json::from_str(&shape_content)
        .context("Failed to parse shape file")?;
    
    match &cli.command {
        Commands::Dump { .. } => {
            dump_shape(&shape, &config_paths, shape_name).await?;
        }
        Commands::Restore { .. } => {
            restore_shape(&shape, &config_paths, shape_name).await?;
        }
        Commands::Run { .. } => {
            if let Some(mappings) = &shape.mappings {
                if !mappings.is_empty() {
                    let migrator = Migrator::new(shape).await?;
                    migrator.run().await?;
                } else {
                    dump_shape(&shape, &config_paths, shape_name).await?;
                }
            } else {
                dump_shape(&shape, &config_paths, shape_name).await?;
            }
        }
    }
    
    Ok(())
}