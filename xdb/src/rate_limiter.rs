//! Rate limiter for controlling background I/O bandwidth.
//!
//! Uses a token bucket algorithm to throttle flush and compaction I/O.
//! Each byte of I/O consumes one token. Tokens are refilled at a
//! configurable rate (bytes per second).

use std::sync::Mutex;
use std::time::Instant;

/// A token bucket rate limiter.
///
/// Thread-safe: can be shared across flush and compaction threads.
///
/// # Example
///
/// ```
/// use xdb::RateLimiter;
///
/// // Limit to 10 MB/s
/// let limiter = RateLimiter::new(10 * 1024 * 1024);
/// limiter.request(4096); // request 4 KB of I/O bandwidth
/// ```
pub struct RateLimiter {
    inner: Mutex<RateLimiterInner>,
    rate_bytes_per_sec: u64,
}

struct RateLimiterInner {
    available_tokens: f64,
    last_refill: Instant,
    total_bytes_through: u64,
    total_requests: u64,
}

impl RateLimiter {
    /// Create a new rate limiter with the given throughput limit.
    ///
    /// `rate_bytes_per_sec` is the maximum number of bytes per second.
    /// A value of 0 means unlimited (no throttling).
    pub fn new(rate_bytes_per_sec: u64) -> Self {
        RateLimiter {
            inner: Mutex::new(RateLimiterInner {
                available_tokens: rate_bytes_per_sec as f64, // start with a full bucket
                last_refill: Instant::now(),
                total_bytes_through: 0,
                total_requests: 0,
            }),
            rate_bytes_per_sec,
        }
    }

    /// Request `bytes` of I/O bandwidth.
    ///
    /// If insufficient tokens are available, this method sleeps until
    /// enough tokens have accumulated. If rate is 0 (unlimited), returns
    /// immediately.
    pub fn request(&self, bytes: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.total_requests += 1;
        inner.total_bytes_through += bytes as u64;

        if self.rate_bytes_per_sec == 0 {
            return; // unlimited — just track stats
        }

        // Refill tokens based on elapsed time.
        let now = Instant::now();
        let elapsed = now.duration_since(inner.last_refill).as_secs_f64();
        inner.available_tokens += elapsed * self.rate_bytes_per_sec as f64;
        // Cap at the rate (one second's worth of tokens).
        if inner.available_tokens > self.rate_bytes_per_sec as f64 {
            inner.available_tokens = self.rate_bytes_per_sec as f64;
        }
        inner.last_refill = now;

        // Consume tokens.
        inner.available_tokens -= bytes as f64;

        // If we're in debt, calculate how long to sleep.
        if inner.available_tokens < 0.0 {
            let deficit = -inner.available_tokens;
            let sleep_secs = deficit / self.rate_bytes_per_sec as f64;
            // Drop the lock before sleeping!
            drop(inner);
            std::thread::sleep(std::time::Duration::from_secs_f64(sleep_secs));
        }
    }

    /// The configured rate in bytes per second.
    pub fn rate(&self) -> u64 {
        self.rate_bytes_per_sec
    }

    /// Total bytes that have passed through the limiter.
    pub fn total_bytes_through(&self) -> u64 {
        self.inner.lock().unwrap().total_bytes_through
    }

    /// Total number of request() calls.
    pub fn total_requests(&self) -> u64 {
        self.inner.lock().unwrap().total_requests
    }

    /// Check if the rate limiter is unlimited (rate == 0).
    pub fn is_unlimited(&self) -> bool {
        self.rate_bytes_per_sec == 0
    }
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("rate_bytes_per_sec", &self.rate_bytes_per_sec)
            .finish()
    }
}

impl std::fmt::Display for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.rate_bytes_per_sec == 0 {
            write!(f, "RateLimiter(unlimited)")
        } else {
            let inner = self.inner.lock().unwrap();
            write!(
                f,
                "RateLimiter(rate={} B/s, through={} B, requests={})",
                self.rate_bytes_per_sec,
                inner.total_bytes_through,
                inner.total_requests,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_returns_immediately() {
        let limiter = RateLimiter::new(0);
        assert!(limiter.is_unlimited());
        limiter.request(1_000_000);
        assert_eq!(limiter.total_bytes_through(), 1_000_000);
        assert_eq!(limiter.total_requests(), 1);
    }

    #[test]
    fn tracks_bytes_and_requests() {
        let limiter = RateLimiter::new(1_000_000); // 1 MB/s
        limiter.request(100);
        limiter.request(200);
        assert_eq!(limiter.total_bytes_through(), 300);
        assert_eq!(limiter.total_requests(), 2);
    }

    #[test]
    fn small_request_within_budget_returns_quickly() {
        let limiter = RateLimiter::new(10_000_000); // 10 MB/s
        let start = Instant::now();
        limiter.request(1000); // 1 KB - well within budget
        let elapsed = start.elapsed();
        assert!(elapsed.as_millis() < 100, "should return quickly");
    }

    #[test]
    fn display_formatting() {
        let limiter = RateLimiter::new(1024);
        assert!(format!("{}", limiter).contains("1024"));

        let unlimited = RateLimiter::new(0);
        assert!(format!("{}", unlimited).contains("unlimited"));
    }

    #[test]
    fn rate_returns_configured_value() {
        let limiter = RateLimiter::new(42);
        assert_eq!(limiter.rate(), 42);
    }
}
