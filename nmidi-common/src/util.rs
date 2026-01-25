use std::time::{SystemTime, UNIX_EPOCH};

/// Generate a unique SSRC based on current time
pub fn generate_ssrc() -> u32 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    (now & 0xFFFFFFFF) as u32
}

/// Generate a unique token based on current time
pub fn generate_token() -> u32 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    ((now >> 32) & 0xFFFFFFFF) as u32
}

/// Get current timestamp in microseconds
pub fn get_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

/// Get the system hostname, falling back to "localhost" if unavailable
pub fn get_hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "localhost".to_string())
}
