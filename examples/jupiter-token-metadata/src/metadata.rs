use {
    carbon_core::error::{CarbonResult, Error as CarbonError},
    jupiter_swap_postgres::db::{JupiterSwapRepository, MintReferenceUpdate},
    log::{debug, error, info, warn},
    reqwest::Client,
    serde::Deserialize,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_commitment_config::CommitmentConfig,
    solana_pubkey::Pubkey,
    sqlx::PgPool,
    std::{
        collections::HashMap,
        env,
        str::FromStr,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::time::sleep,
};

const DEFAULT_TOKEN_URL: &str = "https://lite-api.jup.ag/tokens/v2";
const TOKEN_REQUEST_GAP: Duration = Duration::from_secs(2);
const DEFAULT_BATCH_LIMIT: i64 = 64;
const DEFAULT_STALE_SECS: i64 = 6 * 60 * 60;
const IDLE_WAIT: Duration = Duration::from_secs(8);

pub fn spawn_metadata_sync(pool: PgPool, rpc_url: String) {
    tokio::spawn(async move {
        match MetadataBackfill::bootstrap(pool, rpc_url).await {
            Ok(mut svc) => {
                if let Err(err) = svc.run().await {
                    error!("metadata sync stopped: {err}");
                }
            }
            Err(err) => error!("metadata sync init failed: {err}"),
        }
    });
}

struct MetadataBackfill {
    repository: JupiterSwapRepository,
    rpc_client: Arc<RpcClient>,
    http_client: Client,
    token_url: String,
    last_token_request: Option<Instant>,
    rpc_interval: Option<Duration>,
    last_rpc_request: Option<Instant>,
    batch_limit: i64,
    stale_after_secs: i64,
}

impl MetadataBackfill {
    async fn bootstrap(pool: PgPool, rpc_url: String) -> CarbonResult<Self> {
        let repository = JupiterSwapRepository::new(pool);
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url,
            CommitmentConfig::confirmed(),
        ));
        let http_client = Client::builder()
            .user_agent("carbon-jupiter-token-metadata")
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|err| CarbonError::Custom(err.to_string()))?;
        let token_url = env::var("TOKEN_LIST_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_TOKEN_URL.to_string());
        let rpc_interval = read_rate_limit()
            .filter(|limit| *limit > 0)
            .map(|limit| Duration::from_secs_f64(1.0 / limit as f64));

        Ok(Self {
            repository,
            rpc_client,
            http_client,
            token_url,
            last_token_request: None,
            rpc_interval,
            last_rpc_request: None,
            batch_limit: DEFAULT_BATCH_LIMIT,
            stale_after_secs: DEFAULT_STALE_SECS,
        })
    }

    async fn run(&mut self) -> CarbonResult<()> {
        loop {
            let mints = self
                .repository
                .pending_mints(self.batch_limit, self.stale_after_secs)
                .await?;
            if mints.is_empty() {
                sleep(IDLE_WAIT).await;
                continue;
            }

            let mut token_map = self.fetch_tokens_for(&mints).await;
            for mint in mints {
                if let Err(err) = self.process_mint(&mint, token_map.remove(&mint)).await {
                    warn!("metadata hydrate failed for {mint}: {err}");
                }
            }
        }
    }

    async fn process_mint(
        &mut self,
        mint: &str,
        token_info: Option<TokenCacheEntry>,
    ) -> CarbonResult<()> {
        let mut decimals = token_info.as_ref().and_then(|token| token.decimals);
        let mut last_slot = None;

        if decimals.is_none() {
            if let Some(chain) = self.fetch_onchain_decimals(mint).await? {
                decimals = Some(chain.decimals);
                last_slot = Some(chain.slot);
            }
        }

        let decimals = match decimals {
            Some(value) => value,
            None => {
                debug!("skipping mint {mint}: decimals unavailable");
                return Ok(());
            }
        };

        let symbol = token_info
            .as_ref()
            .and_then(|token| tidy_symbol(token.symbol.as_deref()));
        let name = token_info
            .as_ref()
            .and_then(|token| tidy_label(token.name.as_deref(), 64));
        let category = token_info
            .as_ref()
            .and_then(|token| token.category.as_ref())
            .cloned();

        let mut sources = Vec::new();
        if token_info.is_some() {
            sources.push("jupiter_lite");
        }
        if last_slot.is_some() {
            sources.push("rpc");
        }

        let update = MintReferenceUpdate {
            mint: mint.to_string(),
            decimals,
            symbol,
            name,
            category,
            source: if sources.is_empty() {
                None
            } else {
                Some(sources.join("+"))
            },
            last_updated_slot: last_slot,
        };

        self.repository.upsert_mint_reference_data(update).await?;
        info!("hydrated metadata for {mint}");
        Ok(())
    }

    async fn fetch_tokens_for(&mut self, mints: &[String]) -> HashMap<String, TokenCacheEntry> {
        let mut entries = HashMap::new();
        for chunk in mints.chunks(100) {
            match self.fetch_token_batch(chunk).await {
                Ok(batch) => {
                    entries.extend(batch);
                }
                Err(err) => {
                    warn!("token batch fetch failed: {err}");
                }
            }
        }
        entries
    }

    async fn fetch_token_batch(
        &mut self,
        chunk: &[String],
    ) -> CarbonResult<HashMap<String, TokenCacheEntry>> {
        if chunk.is_empty() {
            return Ok(HashMap::new());
        }

        if let Some(last) = self.last_token_request {
            let elapsed = last.elapsed();
            if elapsed < TOKEN_REQUEST_GAP {
                sleep(TOKEN_REQUEST_GAP - elapsed).await;
            }
        }
        self.last_token_request = Some(Instant::now());

        let query = chunk.join(",");
        let url = format!("{}/search", self.token_url.trim_end_matches('/'));
        let response = self
            .http_client
            .get(url)
            .query(&[("query", query)])
            .send()
            .await
            .map_err(|err| CarbonError::Custom(err.to_string()))?;

        if !response.status().is_success() {
            return Err(CarbonError::Custom(format!(
                "token search returned {}",
                response.status()
            )));
        }

        let tokens: Vec<JupiterLiteToken> = response
            .json()
            .await
            .map_err(|err| CarbonError::Custom(err.to_string()))?;

        let map = tokens
            .into_iter()
            .filter(|token| !token.address.trim().is_empty())
            .map(|token| {
                let decimals = token.decimals.map(|value| value as i32);
                let category = token.tags.first().cloned();
                (
                    token.address,
                    TokenCacheEntry {
                        decimals,
                        symbol: token.symbol,
                        name: token.name,
                        category,
                    },
                )
            })
            .collect();

        Ok(map)
    }

    async fn fetch_onchain_decimals(&mut self, mint: &str) -> CarbonResult<Option<OnChainMint>> {
        if let Some(interval) = self.rpc_interval {
            if let Some(last) = self.last_rpc_request {
                let elapsed = last.elapsed();
                if elapsed < interval {
                    sleep(interval - elapsed).await;
                }
            }
            self.last_rpc_request = Some(Instant::now());
        }

        let pubkey = Pubkey::from_str(mint)
            .map_err(|err| CarbonError::Custom(format!("invalid mint {mint}: {err}")))?;
        let response = self
            .rpc_client
            .get_account_with_commitment(&pubkey, CommitmentConfig::confirmed())
            .await
            .map_err(|err| CarbonError::Custom(err.to_string()))?;
        let Some(account) = response.value else {
            return Ok(None);
        };
        let decimals = extract_mint_decimals(&account.data)?;
        Ok(Some(OnChainMint {
            decimals: decimals as i32,
            slot: response.context.slot,
        }))
    }
}

fn read_rate_limit() -> Option<u32> {
    env::var("RATE_LIMIT").ok().and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            trimmed.parse::<u32>().ok()
        }
    })
}

#[derive(Clone)]
struct TokenCacheEntry {
    decimals: Option<i32>,
    symbol: Option<String>,
    name: Option<String>,
    category: Option<String>,
}

#[derive(Debug)]
struct OnChainMint {
    decimals: i32,
    slot: u64,
}

#[derive(Deserialize)]
struct JupiterLiteToken {
    address: String,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    decimals: Option<u8>,
    #[serde(default)]
    tags: Vec<String>,
}

fn tidy_symbol(value: Option<&str>) -> Option<String> {
    tidy_label(value, 10).map(|s| s.to_ascii_uppercase())
}

fn tidy_label(value: Option<&str>, limit: usize) -> Option<String> {
    let raw = value?.trim();
    if raw.is_empty() {
        return None;
    }
    let cleaned: String = raw
        .chars()
        .filter(|c| !c.is_control())
        .take(limit)
        .collect();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn extract_mint_decimals(data: &[u8]) -> CarbonResult<u8> {
    const DECIMALS_OFFSET: usize = 44;
    if data.len() <= DECIMALS_OFFSET {
        return Err(CarbonError::Custom(
            "mint account data too small to read decimals".to_string(),
        ));
    }
    Ok(data[DECIMALS_OFFSET])
}
