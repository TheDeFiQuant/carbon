# Jupiter Token Metadata Sync

This lightweight worker keeps the `mint_reference_data` table in sync by pulling token info from the Jupiter lite API and falling back to on-chain RPC for missing decimals. It reuses the existing `JupiterSwapRepository` and the same Postgres instance as the swap ingestor.

## Running

```bash
cd examples/jupiter-token-metadata
cargo run
```

Environment variables:

- `DATABASE_URL` – Postgres connection string (same as the swap example)
- `RPC_URL` – Solana RPC endpoint used for the occasional mint account lookup
- `TOKEN_LIST_URL` (optional) – override the default `https://tokens.jup.ag/lite` feed
- `RATE_LIMIT` (optional) – throttle RPC requests to align with your provider (`requests/sec`)

The worker automatically throttles Jupiter API calls to one request every two seconds and respects `RATE_LIMIT` for RPC usage, keeping resource usage slim.
