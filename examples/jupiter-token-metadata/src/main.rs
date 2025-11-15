mod metadata;

use {
    carbon_core::error::{CarbonResult, Error as CarbonError},
    dotenv::{dotenv, from_filename},
    env_logger,
    jupiter_swap_postgres::db,
    sqlx::Pool,
    sqlx_migrator::{Info, Migrate, Migrator, Plan},
    std::env,
    tokio::signal,
};

#[tokio::main]
pub async fn main() -> CarbonResult<()> {
    let _ = from_filename("examples/jupiter-swap-postgres/.env");
    dotenv().ok();
    env_logger::init();

    let db_url = env::var("DATABASE_URL")
        .map_err(|err| CarbonError::Custom(format!("DATABASE_URL must be set ({err})")))?;

    let pool = Pool::<sqlx::Postgres>::connect(&db_url)
        .await
        .map_err(|err| CarbonError::Custom(format!("Failed to connect to Postgres: {err}")))?;

    let mut migrator = Migrator::default();
    migrator
        .add_migration(db::JupiterSwapMigration::boxed())
        .map_err(|err| CarbonError::Custom(format!("Failed to add migration: {err}")))?;
    let mut conn = pool.acquire().await.map_err(|err| {
        CarbonError::Custom(format!("Failed to acquire Postgres connection: {err}"))
    })?;
    migrator
        .run(&mut *conn, &Plan::apply_all())
        .await
        .map_err(|err| CarbonError::Custom(format!("Failed to run migrations: {err}")))?;

    let rpc_url = env::var("RPC_URL")
        .map_err(|err| CarbonError::Custom(format!("RPC_URL must be set ({err})")))?;

    metadata::spawn_metadata_sync(pool.clone(), rpc_url);
    log::info!("Jupiter token metadata sync running. Press Ctrl+C to stop.");
    signal::ctrl_c()
        .await
        .map_err(|err| CarbonError::Custom(format!("Failed to listen for shutdown: {err}")))?;

    Ok(())
}
