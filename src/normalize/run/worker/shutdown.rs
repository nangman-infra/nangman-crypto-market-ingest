use crate::shutdown::ShutdownListener;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;

pub(super) struct ShutdownHandle {
    requested: Arc<AtomicBool>,
    waiter: Option<JoinHandle<()>>,
}

impl ShutdownHandle {
    pub(super) fn install() -> Result<Self, Box<dyn Error>> {
        let requested = Arc::new(AtomicBool::new(false));
        let requested_for_task = Arc::clone(&requested);
        let mut listener = ShutdownListener::new()?;
        let waiter = tokio::spawn(async move {
            listener.wait().await;
            requested_for_task.store(true, Ordering::SeqCst);
        });
        Ok(Self {
            requested,
            waiter: Some(waiter),
        })
    }

    pub(super) fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    pub(super) async fn sleep_or_requested(&mut self, duration: Duration) -> bool {
        if self.is_requested() {
            return true;
        }

        let Some(waiter) = self.waiter.as_mut() else {
            tokio::time::sleep(duration).await;
            return self.is_requested();
        };

        tokio::select! {
            biased;
            _ = waiter => {
                self.requested.store(true, Ordering::SeqCst);
                self.waiter = None;
                true
            }
            _ = tokio::time::sleep(duration) => self.is_requested(),
        }
    }
}

impl Drop for ShutdownHandle {
    fn drop(&mut self) {
        if let Some(waiter) = &self.waiter {
            waiter.abort();
        }
    }
}
