use axum::{
    body::Body,
    extract::{Extension, State},
    http::{Request, Response, StatusCode},
    routing::{get, post},
    Router,
};
use std::time::{Duration, Instant};
use tower::{
    buffer::Buffer,
    cache::{Cache, CacheLayer},
    Service, ServiceExt,
};
use tower_http::{
    services::ServeDir,
    trace::{DefaultOn, TraceLayer},
};
use log::{info, warn, debug};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use rand::Rng;

struct Cache {
    cache: HashMap<String, Vec<u8>>,
}

impl Cache {
    async fn new(size: usize) -> Self {
        Cache {
            cache: HashMap::with_capacity(size),
        }
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, tower_cache::Error> {
        self.cache.get(key).cloned().ok_or(tower_cache::Error::ItemNotFound)
    }

    async fn set(&mut self, key: String, value: Vec<u8>) {
        self.cache.insert(key, value);
    }
}

struct Client {
    client: reqwest::Client,
}

impl Client {
    async fn new() -> Self {
        Client {
            client: reqwest::Client::builder().build().unwrap(),
        }
    }

    async fn request(&self, req: Request<Body>) -> Result<Response<Body>, reqwest::Error> {
        self.client.request(req).await
    }
}

struct Metrics {
    cache_hits: counter::Counter,
    cache_misses: counter::Counter,
    cache_invalidations: counter::Counter,
    cache_size: gauge::Gauge,
    cache_latency: histogram::Histogram,
}

impl Metrics {
    async fn new() -> Self {
        Metrics {
            cache_hits: counter!("cache_hits", "Number of cache hits"),
            cache_misses: counter!("cache_misses", "Number of cache misses"),
            cache_invalidations: counter!("cache_invalidations", "Number of cache invalidations"),
            cache_size: gauge!("cache_size", "Current cache size"),
            cache_latency: histogram!("cache_latency", "Cache latency histogram"),
        }
    }

    async fn display_metrics(&self) {
        info!("Cache hits: {}", self.cache_hits.get());
        info!("Cache misses: {}", self.cache_misses.get());
        info!("Cache invalidations: {}", self.cache_invalidations.get());
        info!("Cache size: {}", self.cache_size.get());
        info!("Cache latency: {:?}", self.cache_latency.get());
    }
}

struct Shard {
    cache: Cache,
    client: Client,
}

impl Shard {
    async fn new(size: usize) -> Self {
        Shard {
            cache: Cache::new(size).await,
            client: Client::new().await,
        }
    }
}

struct AppState {
    shards: Vec<Shard>,
    metrics: Metrics,
    prometheus_handle: PrometheusHandle,
    shutdown_rx: mpsc::Receiver<()>,
    shutdown_handle: JoinHandle<()>,
}

impl AppState {
    async fn new(config: CacheConfig) -> Result<Self, CachingServerError> {
        let mut shards = Vec::new();
        for _ in 0..config.shards {
            let shard = Shard::new(config.size / config.shards).await;
            shards.push(shard);
        }

        let metrics = Metrics::new().await;
        let prometheus_handle = PrometheusBuilder::new()
           .listen("0.0.0.0:9090")
           .unwrap();
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let shutdown_handle = tokio::spawn(async move {
            shutdown_tx.closed().await;
            prometheus_handle.shutdown().await;
        });

        Ok(AppState {
            shards,
            metrics,
            prometheus_handle,
            shutdown_rx,
            shutdown_handle,
        })
    }

    async fn handle_request(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, CachingServerError> {
        let start = Instant::now();
        let key = format!("{}{}{}{}", req.method(), req.uri(), req.query(), req.body().await?.to_vec());

        let shard_id = self.get_shard_id(key);
        let shard = self.shards.get(shard_id).ok_or(CachingServerError::new_other("Shard not found"))?;

        let mut response = match shard.cache.get(&key).await {
            Ok(cached_response) => {
                self.metrics.cache_hits.inc(1);
                info!("Cache hit for {}", key);
                Response::from_data(cached_response.into())
            }
            Err(tower_cache::Error::ItemNotFound) => {
                self.metrics.cache_misses.inc(1);
                info!("Cache miss for {}", key);
                let response = shard.client.request(req).await?;
                let compressed_response = self.compress_response(response).await?;
                shard.cache.set(key, compressed_response.clone()).await?;
                response
            }
            Err(err) => {
                return Err(err.into());
            }
        };

        let body = hyper::body::to_bytes(response.into_body()).await?;
        self.metrics.cache_size.set(self.shards.iter().map(|shard| shard.cache.cache.len()).sum::<usize>() as i64);

        self.metrics.cache_latency.record(start.elapsed().as_secs_f64());
        Ok(response)
    }

    async fn invalidate_cache(&self, key: &str) -> Result<(), CachingServerError> {
        self.metrics.cache_invalidations.inc(1);
        info!("Invalidating cache for {}", key);
        for shard in &self.shards {
            shard.cache.cache.remove(key);
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), CachingServerError> {
        self.shutdown_rx.close().await;
        self.shutdown_handle.await?;
        Ok(())
    }

    async fn compress_response(&self, response: Response<Body>) -> Result<Vec<u8>, CachingServerError> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&response.into_body().await?.to_vec()).await?;
        let compressed_response = encoder.finish().await?;
        Ok(compressed_response)
    }

    fn get_shard_id(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        hasher.write(key.as_bytes());
        let hash = hasher.finish();
        (hash % self.shards.len()) as usize
    }

    async fn balance_load(&self) {
        // implement load balancing logic here
        // for now, just shuffle the shards
        let mut rng = rand::thread_rng();
        let mut shards = self.shards.clone();
        shards.shuffle(&mut rng);
        self.shards = shards;
    }
}

struct CacheConfig {
    ttl: Duration,
    size: usize,
    eviction_policy: EvictionPolicy,
    shards: usize,
}

enum EvictionPolicy {
    Lru,
    Mru,
    Random,
    Lfu,
    Mfu,
}

#[tokio::main]
async fn main() -> Result<(), CachingServerError> {
    env_logger::init();

    let config = CacheConfig {
        ttl: Duration::from_secs(3600), // 1 hour cache TTL
        size: 1000, // cache size limit
        eviction_policy: EvictionPolicy::Lru,
        shards: 5, // 5 shards
    };

    let app_state = AppState::new(config).await?;

    let app = Router::new()
       .route("/", get(handle_index))
       .route("/healthz", get(handle_healthz))
       .route("/metrics", get(handle_metrics))
       .layer(TraceLayer::new_for_http())
       .layer(Extension(app_state.clone()))
       .layer(CacheLayer::new(app_state.shards[0].cache.clone()));

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("Starting server on {}", addr);
    axum::Server::bind(&addr)
       .serve(app.into_make_service())
       .await
       .unwrap();

    Ok(())
}

async fn handle_index(Extension(state): Extension<AppState>) -> Result<Response<Body>, CachingServerError> {
    let response = state.shards[0].client.request(Request::new(Body::empty())).await?;
    let body = hyper::body::to_bytes(response.into_body()).await?;
    Ok(Response::from_data(Body::from(body)))
}

async fn handle_healthz(Extension(state): Extension<AppState>) -> Result<Response<Body>, CachingServerError> {
    Ok(Response::new(Body::empty()))
}

async fn handle_metrics(Extension(state): Extension<AppState>) -> Result<Response<Body>, CachingServerError> {
    state.metrics.display_metrics().await;
    let response = ServeDir::new(".")
       .handle_concurrent(10)
       .serve(Request::new(Body::empty()))
       .await?;

    Ok(response)
}
