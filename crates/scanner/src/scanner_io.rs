use std::time::SystemTime;
use std::{fmt::Debug, sync::Arc};

use crate::{DataUsageCache, DataUsageInfo};
use futures::future::join_all;
use rustfs_common::heal_channel::HealScanMode;
use rustfs_ecstore::error::Error;
use rustfs_ecstore::set_disk::SetDisks;
use rustfs_ecstore::store_api::BucketOptions;
use rustfs_ecstore::{StorageAPI, error::Result, store::ECStore};
use tokio::sync::{Mutex, mpsc};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::error;

#[async_trait::async_trait]
pub trait ScannerIO: Send + Sync + Debug + 'static {
    async fn nsscanner(
        &self,
        ctx: CancellationToken,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()>;
}

#[async_trait::async_trait]
pub trait ScannerIOCache: Send + Sync + Debug + 'static {
    async fn nsscanner_cache(
        &self,
        ctx: CancellationToken,
        updates: mpsc::Sender<DataUsageCache>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()>;
}

#[async_trait::async_trait]
impl ScannerIO for ECStore {
    async fn nsscanner(
        &self,
        ctx: CancellationToken,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()> {
        let child_token = ctx.child_token();

        let all_buckets = self.list_bucket(&BucketOptions::default()).await?;

        if all_buckets.is_empty() {
            if let Err(e) = updates.send(DataUsageInfo::default()).await {
                error!("Failed to send data usage info: {}", e);
            }
            return Ok(());
        }

        let mut total_results = 0;
        for pool in self.pools.iter() {
            total_results += pool.disk_set.len();
        }

        let results = vec![DataUsageCache::default(); total_results];
        let results_mutex: Arc<Mutex<Vec<DataUsageCache>>> = Arc::new(Mutex::new(results));
        let first_err_mutex: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
        let mut results_index: i32 = -1_i32;
        let mut wait_futs = Vec::new();

        for pool in self.pools.iter() {
            for set in pool.disk_set.iter() {
                results_index += 1;

                let results_index_clone = results_index as usize;
                // Clone the Arc to move it into the spawned task
                let set_clone: Arc<SetDisks> = Arc::clone(set);

                let child_token_clone = child_token.clone();
                let want_cycle_clone = want_cycle;
                let scan_mode_clone = scan_mode;
                let results_mutex_clone = results_mutex.clone();
                let first_err_mutex_clone = first_err_mutex.clone();

                let (tx, mut rx) = tokio::sync::mpsc::channel::<DataUsageCache>(1);

                // Spawn task to receive and store results
                let receiver_fut = tokio::spawn(async move {
                    while let Some(result) = rx.recv().await {
                        let mut results = results_mutex_clone.lock().await;
                        results[results_index_clone] = result;
                    }
                });
                wait_futs.push(receiver_fut);

                // Spawn task to run the scanner
                let scanner_fut = tokio::spawn(async move {
                    if let Err(e) = set_clone
                        .nsscanner_cache(child_token_clone.clone(), tx, want_cycle_clone, scan_mode_clone)
                        .await
                    {
                        error!("Failed to scan set: {e}");
                        let _ = first_err_mutex_clone.lock().await.insert(e);
                        child_token_clone.cancel();
                    }
                });
                wait_futs.push(scanner_fut);
            }
        }

        let (update_tx, mut update_rx) = tokio::sync::oneshot::channel::<()>();

        let all_buckets_clone = all_buckets.iter().map(|b| b.name.clone()).collect::<Vec<String>>();
        tokio::spawn(async move {
            let mut last_update = SystemTime::now();

            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = child_token.cancelled() => {
                        break;
                    }
                    res = &mut update_rx => {
                        if res.is_err() {
                            break;
                        }

                        let results = results_mutex.lock().await;
                        let mut all_merged = DataUsageCache::default();
                        for result in results.iter() {
                            if result.info.last_update.is_none() {
                                return;
                            }
                            all_merged.merge(result);
                        }

                        if all_merged.root().is_some() && all_merged.info.last_update.unwrap() > last_update {
                           if let Err(e) = updates
                                .send(all_merged.dui(&all_merged.info.name, &all_buckets_clone))
                                .await {
                                error!("Failed to send data usage info: {}", e);
                            }
                        }
                        break;
                    }
                    _ = ticker.tick() => {
                        let results = results_mutex.lock().await;
                        let mut all_merged = DataUsageCache::default();
                        for result in results.iter() {
                            if result.info.last_update.is_none() {
                                return;
                            }
                            all_merged.merge(result);
                        }

                        if all_merged.root().is_some() && all_merged.info.last_update.unwrap() > last_update {
                           if let Err(e) = updates
                                .send(all_merged.dui(&all_merged.info.name, &all_buckets_clone))
                                .await {
                                error!("Failed to send data usage info: {}", e);
                            }
                            last_update = all_merged.info.last_update.unwrap();
                        }
                    }
                }
            }
        });

        let _ = join_all(wait_futs).await;

        let _ = update_tx.send(());

        Ok(())
    }
}

#[async_trait::async_trait]
impl ScannerIOCache for SetDisks {
    async fn nsscanner_cache(
        &self,
        ctx: CancellationToken,
        updates: mpsc::Sender<DataUsageCache>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()> {
        Ok(())
    }
}
