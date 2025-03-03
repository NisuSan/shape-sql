use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sqlx::{Pool, Row, any::AnyRow};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tokio::fs::File as TokioFile;
use tokio::io::{BufWriter, AsyncWriteExt};
use async_compression::tokio::write::GzipEncoder;
use sysinfo::System;
use tracing::{info, warn, error};
use futures_util::stream::StreamExt;
use jsonschema;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Config {
    source: SourceConfig,
    mappings: Vec<Mapping>,
    #[serde(default)]
    transforms: HashMap<String, String>,
    #[serde(default)]
    compress: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SourceConfig {
    #[serde(rename = "type")]
    db_type: String,
    connection: String,
    tables: Vec<TableConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TableConfig {
    name: String,
    #[serde(default)]
    where_clause: Option<HashMap<String, JsonValue>>,
    #[serde(default)]
    chunk_size: Option<JsonValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Mapping {
    source_table: String,
    target_table: String,
    connection: String,
    columns: Vec<ColumnMapping>,
    #[serde(default)]
    apply_transformers: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ColumnMapping {
    source: String,
    target: String,
    #[serde(rename = "type", default)]
    col_type: Option<String>,
    #[serde(default)]
    transform: Option<String>,
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

struct Migrator {
    config: Config,
    transformers: HashMap<String, TransformerType>,
    source_pool: Pool<sqlx::Any>,
    source_type: DbType,
    target_pools: HashMap<String, (DbType, Pool<sqlx::Any>)>,
}

impl Migrator {
    async fn new(config: Config) -> Result<Self> {
        tracing_subscriber::fmt::init();

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
        let mut unique_connections = HashSet::new();
        for mapping in &config.mappings {
            if unique_connections.insert(mapping.connection.clone()) {
                let db_type = DbType::from_connection_string(&mapping.connection)?;
                let pool = sqlx::any::AnyPoolOptions::new()
                    .connect(&mapping.connection)
                    .await
                    .context(format!("Failed to connect to target '{}'", mapping.connection))?;
                target_pools.insert(mapping.connection.clone(), (db_type, pool));
            }
        }

        let migrator = Self {
            config,
            transformers,
            source_pool,
            source_type,
            target_pools,
        };

        migrator.apply_or_suggest_transformers().await?;
        for mapping in &migrator.config.mappings {
            if let Some((_, pool)) = migrator.target_pools.get(&mapping.connection) {
                migrator.validate_target_schema(pool, mapping).await?;
            } else {
                warn!("Target pool not initialized for '{}'", mapping.connection);
            }
        }

        Ok(migrator)
    }

    async fn apply_or_suggest_transformers(&self) -> Result<()> {
        let mut config = self.config.clone();
        for mapping in &mut config.mappings {
            let _sample = self.sample_data(&mapping.source_table).await;
            for col in &mut mapping.columns {
                if col.transform.is_none() {
                    let col_name = col.source.to_lowercase();
                    let suggestions: Vec<&str> = match col_name.as_str() {
                        s if s.contains("at") || s.contains("date") || s.contains("time") => vec!["normalize_timestamp"],
                        s if s.contains("email") || s.contains("user") || s.contains("name") || s.contains("title") => {
                            let mut suggs = Vec::new();
                            if s.contains("email") || s.contains("user") {
                                suggs.push("to_lowercase");
                            }
                            if s.contains("name") || s.contains("title") {
                                suggs.push("trim");
                            }
                            suggs
                        }
                        s if s.contains("price") || s.contains("score") => vec!["round"],
                        _ => vec![],
                    };

                    if !suggestions.is_empty() {
                        if mapping.apply_transformers {
                            if let Some(suggestion) = suggestions.iter().find(|&&s| s != "round") {
                                col.transform = Some(suggestion.to_string());
                                info!("Applied '{}' to {}.{}", suggestion, mapping.source_table, col.source);
                            }
                        }
                        info!(
                            "Suggested transformers for {}.{}: {}. {}",
                            mapping.source_table, col.source, suggestions.join(", "),
                            if mapping.apply_transformers { "Applied where applicable." } else { "Set 'apply_transformers' to apply." }
                        );
                    }
                }
            }
        }
        Ok(())
    }

    async fn sample_data(&self, table: &str) -> Vec<AnyRow> {
        let query = format!("SELECT * FROM {} LIMIT 100", table);
        sqlx::query(&query).fetch_all(&self.source_pool).await.unwrap_or_default()
    }

    async fn validate_target_schema(&self, pool: &Pool<sqlx::Any>, mapping: &Mapping) -> Result<()> {
        let target_type = DbType::from_connection_string(&mapping.connection)?;
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
            let inferred = self.infer_type(&mapping.source_table, &col.source, col.col_type.as_deref(), Some(&target_type));
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
            ("INTEGER", "BIGINT") | ("BIGINT", "INTEGER") | ("TEXT", "VARCHAR") | ("TIMESTAMP", "TIMESTAMP") | ("TIMESTAMP", "DATETIME") | ("DATETIME", "TIMESTAMP") | ("JSON", "JSON")
        )
    }

    fn calculate_chunk_size(chunk_size: Option<&JsonValue>) -> usize {
        match chunk_size {
            Some(JsonValue::String(s)) if s.to_lowercase() == "auto" => {
                let mut system = System::new_all();
                system.refresh_memory();
                let _available_memory = system.available_memory();
                let estimated_row_size = 1024;
                let target_chunk_memory = 1 * 1024 * 1024;
                let chunk_size = (target_chunk_memory / estimated_row_size) as usize;
                chunk_size.clamp(100, 10_000)
            }
            Some(JsonValue::Number(n)) => n.as_u64().unwrap_or(1000) as usize,
            _ => Self::calculate_chunk_size(Some(&JsonValue::String("auto".to_string()))),
        }
    }

    fn build_select_query(table: &TableConfig) -> (String, Vec<String>) {
        let mut query = format!("SELECT ? as table_name, * FROM {}", table.name);
        let mut params = vec![table.name.clone()];
        if let Some(where_clause) = &table.where_clause {
            let mut conditions: Vec<String> = Vec::new();
            for (key, value) in where_clause {
                let is_or = key.starts_with("||");
                let column = if is_or { &key[2..] } else { key.as_str() };
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
            }.to_string(),
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
            }.to_string(),
        }
    }

    async fn run(&self) -> Result<()> {
        let (tx, rx) = mpsc::channel::<(String, String, Vec<RowData>)>(100);

        for table in &self.config.source.tables {
            let tx = tx.clone();
            let source_pool = self.source_pool.clone();
            let table_config = table.clone();
            let mappings = self.config.mappings.clone();
            let transformers = self.transformers.clone();
            let chunk_size = Self::calculate_chunk_size(table_config.chunk_size.as_ref());
            let source_type = self.source_type.clone();

            let (query, params) = Self::build_select_query(&table_config);
            info!("Query for {}: {}", table_config.name, query);
            info!("Using chunk_size: {} for table {}", chunk_size, table_config.name);

            tokio::spawn(async move {
                let mut q = sqlx::query(&query);
                for param in params {
                    q = q.bind(param);
                }
                let mut rows = q.fetch(&source_pool);
                let mut batch = Vec::new();

                while let Some(row) = rows.next().await.transpose()? {
                    let transformed = Self::transform_row(&row, &mappings, &transformers, source_type.clone())?;
                    batch.push(transformed);

                    if batch.len() >= chunk_size {
                        for mapping in &mappings {
                            if mapping.source_table == table_config.name {
                                let target_table = mapping.target_table.clone();
                                let target_connection = mapping.connection.clone();
                                tx.send((target_connection, target_table, batch)).await?;
                                batch = Vec::new();
                                break;
                            }
                        }
                    }
                }

                if !batch.is_empty() {
                    for mapping in &mappings {
                        if mapping.source_table == table_config.name {
                            let target_table = mapping.target_table.clone();
                            let target_connection = mapping.connection.clone();
                            tx.send((target_connection, target_table, batch)).await?;
                            break;
                        }
                    }
                }

                Ok::<_, anyhow::Error>(())
            });
        }

        drop(tx);

        if !self.target_pools.is_empty() {
            let mut rx = rx;
            let target_pools = self.target_pools.clone();
            while let Some((target_connection, table, batch)) = rx.recv().await {
                if let Some((db_type, pool)) = target_pools.get(&target_connection) {
                    if let Err(e) = Self::write_batch_to_db(pool, *db_type, &table, &batch).await {
                        error!("Failed to write batch to {} (target: {}): {}", table, target_connection, e);
                    } else {
                        info!("Wrote {} rows to {} (target: {})", batch.len(), table, target_connection);
                    }
                } else {
                    error!("No pool found for target '{}'", target_connection);
                }
            }
        } else {
            let filename = if self.config.compress { "dump.sql.gz" } else { "dump.sql" };
            let file = TokioFile::create(filename).await
                .context(format!("Failed to create {}", filename))?;
            let buffered_writer = BufWriter::with_capacity(4 * 1024 * 1024, file);
            let mut writer = if self.config.compress {
                Writer::Compressed(GzipEncoder::new(buffered_writer))
            } else {
                Writer::Plain(buffered_writer)
            };

            let mut rx = rx;
            while let Some((_, target_table, batch)) = rx.recv().await {
                self.write_batch_to_file(&mut writer, &target_table, &batch).await?;
                match &mut writer {
                    Writer::Plain(w) => w.flush().await?,
                    Writer::Compressed(w) => w.flush().await?,
                }
                info!("Dumped {} rows for {}", batch.len(), target_table);
            }
        }

        Ok(())
    }

    async fn write_batch_to_db(pool: &Pool<sqlx::Any>, db_type: DbType, table: &str, batch: &[RowData]) -> Result<()> {
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

        if db_type == DbType::MySql {
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

    async fn write_batch_to_file(&self, writer: &mut Writer, table: &str, batch: &[RowData]) -> Result<()> {
        let mut query = format!("INSERT INTO {} (", table);
        let columns: Vec<String> = batch[0].keys().cloned().collect();
        query.push_str(&columns.join(", "));
        query.push_str(") VALUES ");

        let mut values = Vec::new();
        for row in batch {
            let row_values: Vec<String> = columns
                .iter()
                .map(|col| match &row[col] {
                    JsonValue::Number(n) => n.to_string(),
                    JsonValue::String(s) => format!("'{}'", s.replace("'", "''")),
                    JsonValue::Bool(b) => b.to_string(),
                    JsonValue::Object(_) | JsonValue::Array(_) => serde_json::to_string(&row[col]).unwrap_or("NULL".to_string()),
                    _ => "NULL".to_string(),
                })
                .collect();
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

    fn transform_row(row: &AnyRow, mappings: &[Mapping], transformers: &HashMap<String, TransformerType>, source_type: DbType) -> Result<RowData> {
        let mapping = mappings
            .iter()
            .find(|m| m.source_table == row.get::<String, _>("table_name"))
            .context("No mapping found for source table")?;
        let target_type = DbType::from_connection_string(&mapping.connection)?;

        let mut data = RowData::new();
        for col in &mapping.columns {
            let col_type = col.col_type.as_deref().unwrap_or("string");
            let value = match col_type {
                "i32" | "INTEGER" => row.try_get::<i32, _>(col.source.as_str()).map(|v| v.into()).map_err(|e| {
                    error!("Transform error {}.{}: {}", mapping.source_table, col.source, e);
                    e
                }).unwrap_or(JsonValue::Null),
                "i64" | "BIGINT" => row.try_get::<i64, _>(col.source.as_str()).map(|v| v.into()).map_err(|e| {
                    error!("Transform error {}.{}: {}", mapping.source_table, col.source, e);
                    e
                }).unwrap_or(JsonValue::Null),
                "f64" | "DOUBLE PRECISION" => row.try_get::<f64, _>(col.source.as_str()).map(|v| v.into()).map_err(|e| {
                    error!("Transform error {}.{}: {}", mapping.source_table, col.source, e);
                    e
                }).unwrap_or(JsonValue::Null),
                "string" | "TEXT" => row.try_get::<String, _>(col.source.as_str()).map(|v| v.into()).map_err(|e| {
                    error!("Transform error {}.{}: {}", mapping.source_table, col.source, e);
                    e
                }).unwrap_or(JsonValue::Null),
                "timestamp" | "TIMESTAMP" => row.try_get::<String, _>(col.source.as_str())
                    .map(|v| v.into()).map_err(|e| {
                        error!("Transform error {}.{}: {}", mapping.source_table, col.source, e);
                        e
                    }).unwrap_or(JsonValue::Null),
                "bytes" | "BYTEA" => row.try_get::<Vec<u8>, _>(col.source.as_str())
                    .map(|v| serde_json::to_value(v).unwrap_or(JsonValue::Null)).map_err(|e| {
                        error!("Transform error {}.{}: {}", mapping.source_table, col.source, e);
                        e
                    }).unwrap_or(JsonValue::Null),
                "json" | "JSONB" => row.try_get::<String, _>(col.source.as_str())
                    .map(|v| serde_json::from_str(&v).unwrap_or(JsonValue::Null))
                    .map_err(|e| {
                        error!("Transform error {}.{}: {}", mapping.source_table, col.source, e);
                        e
                    }).unwrap_or(JsonValue::Null),
                "bool" | "BOOLEAN" => row.try_get::<bool, _>(col.source.as_str()).map(|v| v.into()).map_err(|e| {
                    error!("Transform error {}.{}: {}", mapping.source_table, col.source, e);
                    e
                }).unwrap_or(JsonValue::Null),
                _ => return Err(anyhow::anyhow!("Unsupported type: {}", col_type)),
            };

            let transformed_value = if let Some(transform) = &col.transform {
                transformers
                    .get(transform)
                    .ok_or_else(|| anyhow::anyhow!("Transformer {} not found", transform))?
                    .transform(value)?
            } else {
                value
            };

            data.insert(col.target.clone(), transformed_value);
        }

        Ok(data)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config_file = std::fs::read_to_string("schema.transit.json")
        .context("Failed to read schema.transit.json")?;
    
    let schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["source", "mappings"],
        "properties": {
            "source": { "type": "object" },
            "mappings": { "type": "array" },
            "transforms": { "type": "object" },
            "compress": { "type": "boolean" }
        }
    });

    let config_json: serde_json::Value = serde_json::from_str(&config_file)?;
    if jsonschema::validate(&schema, &config_json).is_err() {
        return Err(anyhow::anyhow!("Invalid schema.transit.json"));
    }

    let config: Config = serde_json::from_str(&config_file)
        .context("Failed to parse schema.transit.json")?;

    let migrator = Migrator::new(config).await?;
    migrator.run().await?;

    info!("Migration or dump completed successfully!");
    Ok(())
}