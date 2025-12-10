use std::{fmt::Debug, sync::Arc};

use rustfs_common::heal_channel::HealScanMode;
use rustfs_ecstore::{StorageAPI, store::ECStore};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::DataUsageInfo;
use crate::ScannerError;

#[async_trait::async_trait]
pub trait ScannerIO: Send + Sync + Debug + 'static {
    async fn nsscanner(
        &self,
        ctx: CancellationToken,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<(), ScannerError>;
}

#[async_trait::async_trait]
impl<T: StorageAPI> ScannerIO for T {
    async fn nsscanner(
        &self,
        _ctx: CancellationToken,
        _updates: mpsc::Sender<DataUsageInfo>,
        _want_cycle: u64,
        _scan_mode: HealScanMode,
    ) -> Result<(), ScannerError> {
        // TODO: Implement NSScanner
        Ok(())
    }
}
