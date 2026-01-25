use crate::{AppleMidiPacket, util::{get_timestamp, micros_to_rtp_timestamp}};
use std::net::SocketAddr;
use tracing::debug;

/// Session state for an AppleMIDI peer
#[derive(Debug, Clone)]
pub struct SessionState {
    pub ssrc: u32,
    pub token: u32,
    pub addr: SocketAddr,
    pub data_addr: SocketAddr,
    pub sequence: u16,
    pub timestamp: u32,
    /// Time offset in RTP ticks (10 kHz, 100us): peer_time = local_time + offset
    /// Calculated from AppleMIDI synchronization packets
    pub time_offset_ticks: i64,
    /// Last synchronization count received from peer
    pub last_sync_count: u8,
    /// Last seen MIDI status byte for running status reconstruction
    pub last_status: Option<u8>,
}

/// Result of handling a synchronization packet
#[derive(Debug, Clone)]
pub struct SyncResult {
    /// Response packet to send (if any)
    pub response: Option<AppleMidiPacket>,
    /// Updated time offset in ticks (if calculated)
    pub time_offset_ticks: Option<i64>,
}

/// Handle incoming synchronization packet and produce response
/// 
/// Uses a simplified 2-timestamp approach for time offset calculation rather than
/// the full 3-timestamp method. This approach is more robust in practice:
/// - timestamp1 (peer's send time) and timestamp2 (our original request time) provide
///   a reliable RTT measurement
/// - timestamp3 (peer's response generation time) adds complexity without significantly
///   improving accuracy in typical network conditions
/// - The simpler approach matches the reference implementation and avoids potential
///   issues with asymmetric delays
/// 
/// # Arguments
/// * `ssrc` - Local SSRC
/// * `peer_ssrc` - Peer SSRC
/// * `count` - Sync count from packet
/// * `timestamp1` - Peer's send time
/// * `timestamp2` - Our original send time (from our request)
/// * `_timestamp3` - Peer's response time (not used in simplified calculation)
/// 
/// Returns SyncResult with optional response packet and time offset
pub fn handle_synchronization(
    ssrc: u32,
    peer_ssrc: u32,
    count: u8,
    timestamp1: u64,
    timestamp2: u64,
    _timestamp3: u64,
) -> SyncResult {
    let now_ticks = micros_to_rtp_timestamp(get_timestamp()) as u64;
    
    let time_offset_ticks = if timestamp2 > 0 {
        // This is a response to our sync request (count is odd)
        // Calculate time offset: peer_time = local_time + offset
        // timestamp1 = peer's send time
        // timestamp2 = our original send time
        // timestamp3 = peer's response time (not used in current calculation)
        // now = our current time
        
        // Round trip time
        let rtt = now_ticks.wrapping_sub(timestamp2);
        
        // Estimated one-way latency (half of RTT)
        let latency = rtt / 2;
        
        // Peer's time when we received this response
        let peer_time_estimate = timestamp1.wrapping_add(latency);
        
        // Calculate offset: how much to add to local time to get peer time
        // Compute signed offset in ticks; wrap-aware within u32 range
        let raw_diff = peer_time_estimate.wrapping_sub(now_ticks) as i64;
        let wrap = (u32::MAX as i64) + 1;
        let time_offset = if raw_diff > wrap / 2 {
            raw_diff - wrap
        } else if raw_diff < -wrap / 2 {
            raw_diff + wrap
        } else {
            raw_diff
        };
        
        debug!(
            "Sync from {}: offset={}, rtt={}, latency={}",
            peer_ssrc, time_offset, rtt, latency
        );
        
        Some(time_offset)
    } else {
        None
    };
    
    // Generate response per AppleMIDI spec
    let response = match count {
        0 => Some(AppleMidiPacket::Synchronization {
            ssrc,
            count: 1,
            timestamp1,
            timestamp2: now_ticks,
            timestamp3: now_ticks,
        }),
        1 => Some(AppleMidiPacket::Synchronization {
            ssrc,
            count: 2,
            timestamp1,
            timestamp2,
            timestamp3: now_ticks,
        }),
        _ => None,
    };
    
    SyncResult {
        response,
        time_offset_ticks,
    }
}

/// Create initial synchronization request packet
pub fn create_sync_request(ssrc: u32) -> AppleMidiPacket {
    let now_ticks = micros_to_rtp_timestamp(get_timestamp()) as u64;
    AppleMidiPacket::Synchronization {
        ssrc,
        count: 0,
        timestamp1: now_ticks,
        timestamp2: 0,
        timestamp3: 0,
    }
}
