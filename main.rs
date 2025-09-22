use rusqlite::{Connection, params};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use warp::Filter;
use tokio;
use ethers::prelude::*;
use ethers::types::{Address, Filter, Log};
use std::str::FromStr;
use tokio_stream::StreamExt;

#[derive(Deserialize)]
struct Config {
    polygon: Polygon,
    token: Token,
    exchanges: Exchanges,
}

#[derive(Deserialize)]
struct Polygon { rpc_url: String }
#[derive(Deserialize)]
struct Token { pol_address: String }
#[derive(Deserialize)]
struct Exchanges { binance: Vec<String> }

#[tokio::main]
async fn main() {
    // Load config.toml
    let config_text = fs::read_to_string("config.toml").expect("Cannot read config.toml");
    let config: Config = toml::from_str(&config_text).expect("Invalid config.toml");

    // Open SQLite DB
    let conn = Connection::open("netflow.db").expect("DB open failed");

    // Create tables
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_number BIGINT,
            tx_hash TEXT,
            from_address TEXT,
            to_address TEXT,
            amount TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS netflows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT,
            inflow TEXT,
            outflow TEXT,
            cumulative_netflow TEXT,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        );"
    ).unwrap();

    // Store Binance addresses in a set
    let binance_addresses: HashSet<String> = config.exchanges.binance.into_iter().collect();
    println!("✅ Loaded {} Binance addresses", binance_addresses.len());

    // Start blockchain listener in background
    let rpc_url = config.polygon.rpc_url.clone();
    let pol_addr = config.token.pol_address.clone();
    let binance_set = binance_addresses.clone();
    let conn_clone = conn.clone();

    tokio::spawn(async move {
        listen_transfers(&rpc_url, &pol_addr, &binance_set, &conn_clone)
            .await
            .expect("Listener crashed");
    });

    // Simulate some flow for demonstration
    simulate_flow(&conn, "binance", "1000", "200");

    // Simple HTTP API to fetch latest netflow
    let route = warp::path("netflow")
        .and(warp::get())
        .map(move || {
            let conn = Connection::open("netflow.db").unwrap();
            let mut stmt = conn.prepare(
                "SELECT exchange, inflow, outflow, cumulative_netflow, last_updated 
                 FROM netflows ORDER BY id DESC LIMIT 1"
            ).unwrap();
            let row = stmt.query_row([], |row| {
                Ok(serde_json::json!({
                    "exchange": row.get::<_, String>(0)?,
                    "inflow": row.get::<_, String>(1)?,
                    "outflow": row.get::<_, String>(2)?,
                    "cumulative_netflow": row.get::<_, String>(3)?,
                    "last_updated": row.get::<_, String>(4)?,
                }))
            }).unwrap();
            warp::reply::json(&row)
        });

    println!("🌐 API running at http://127.0.0.1:3030/netflow");
    warp::serve(route).run(([127, 0, 0, 1], 3030)).await;
}

// Simulate some netflow for demonstration purposes
fn simulate_flow(conn: &Connection, exchange: &str, inflow: &str, outflow: &str) {
    conn.execute(
        "INSERT INTO transactions (block_number, tx_hash, from_address, to_address, amount) 
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![123456, "0xtesthash", "0xfrom", "0xto", inflow],
    ).unwrap();

    let cumulative: i128 = inflow.parse::<i128>().unwrap() - outflow.parse::<i128>().unwrap();
    conn.execute(
        "INSERT INTO netflows (exchange, inflow, outflow, cumulative_netflow) 
         VALUES (?1, ?2, ?3, ?4)",
        params![exchange, inflow, outflow, cumulative.to_string()],
    ).unwrap();
}

// Listen for real POL transfers in real-time
async fn listen_transfers(
    rpc_url: &str,
    pol_address: &str,
    binance_addresses: &HashSet<String>,
    conn: &Connection,
) -> anyhow::Result<()> {
    let provider = Provider::<Http>::try_from(rpc_url)?;
    let provider = std::sync::Arc::new(provider);

    // Parse POL token contract address
    let pol_addr: Address = Address::from_str(pol_address)?;

    // ERC20 Transfer event signature
    let transfer_sig = H256::from_slice(&keccak256("Transfer(address,address,uint256)"));

    // Filter logs for this token
    let filter = Filter::new().address(pol_addr).event(&transfer_sig);

    let mut stream = provider.subscribe_logs(&filter).await?;

    println!("🔍 Listening for POL transfers...");

    while let Some(log) = stream.next().await {
        handle_transfer_log(&conn, &log, binance_addresses)?;
    }

    Ok(())
}

// Decode and process each transfer
fn handle_transfer_log(
    conn: &Connection,
    log: &Log,
    binance_addresses: &HashSet<String>,
) -> anyhow::Result<()> {
    let from = format!("0x{}", hex::encode(&log.topics[1].as_bytes()[12..]));
    let to = format!("0x{}", hex::encode(&log.topics[2].as_bytes()[12..]));
    let amount: U256 = U256::from_big_endian(&log.data.0);

    let mut inflow: i128 = 0;
    let mut outflow: i128 = 0;

    if binance_addresses.contains(&to) {
        inflow = amount.as_u128() as i128;
        println!("📥 Deposit {} POL to Binance", inflow);
    } else if binance_addresses.contains(&from) {
        outflow = amount.as_u128() as i128;
        println!("📤 Withdrawal {} POL from Binance", outflow);
    }

    if inflow != 0 || outflow != 0 {
        let cumulative = inflow - outflow;
        conn.execute(
            "INSERT INTO netflows (exchange, inflow, outflow, cumulative_netflow) 
             VALUES (?1, ?2, ?3, ?4)",
            params!["binance", inflow.to_string(), outflow.to_string(), cumulative.to_string()],
        )?;
    }

    Ok(())
}

