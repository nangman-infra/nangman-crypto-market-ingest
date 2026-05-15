use std::io;

pub struct ShutdownListener {
    #[cfg(unix)]
    sigterm: tokio::signal::unix::Signal,
}

impl ShutdownListener {
    pub fn new() -> io::Result<Self> {
        #[cfg(unix)]
        {
            Ok(Self {
                sigterm: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
            })
        }

        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    pub async fn wait(&mut self) {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = self.sigterm.recv() => {}
            }
        }

        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}
