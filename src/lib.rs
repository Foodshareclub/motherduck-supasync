//! # MotherDuck SupaSync
//!
//! A robust Rust library for syncing Supabase PostgreSQL data to MotherDuck for analytics.
//!
//! ## Features
//!
//! - **Incremental sync**: Only sync records that haven't been synced yet
//! - **Full sync**: Re-sync all records
//! - **Retry logic**: Automatic retries with exponential backoff
//! - **Schema management**: Automatic table creation and migrations
//! - **Batch processing**: Efficient batch inserts for large datasets
//! - **Progress tracking**: Real-time progress updates via callbacks
//! - **Metrics**: Built-in metrics for observability
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use motherduck_supasync::{SyncClient, SyncConfig, SyncMode};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = SyncConfig::builder()
//!         .postgres_url("postgres://user:pass@host:5432/db")
//!         .motherduck_token("your_token")
//!         .motherduck_database("analytics")
//!         .build()?;
//!
//!     let client = SyncClient::new(config).await?;
//!     let result = client.sync(SyncMode::Incremental).await?;
//!
//!     println!("Synced {} records", result.total_records());
//!     Ok(())
//! }
//! ```
//!
//! ## Custom Tables
//!
//! You can define custom table mappings:
//!
//! ```rust,no_run
//! use motherduck_supasync::{SyncClient, SyncConfig, TableMapping};
//!
//! let mapping = TableMapping::builder()
//!     .source_table("my_source_table")
//!     .target_table("my_target_table")
//!     .primary_key(["id"])
//!     .sync_flag_column("synced")
//!     .build();
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![deny(unsafe_code)]

pub mod config;
pub mod error;
pub mod metrics;
pub mod motherduck;
pub mod postgres;
pub mod schema;
pub mod sync;

// Re-exports for convenience
pub use config::{SyncConfig, SyncConfigBuilder, TableMapping, TableMappingBuilder};
pub use error::{Error, Result};
pub use motherduck::MotherDuckClient;
pub use schema::{Column, ColumnType, Schema};
pub use sync::{SyncClient, SyncMode, SyncProgress, SyncResult};

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
