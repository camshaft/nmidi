use anyhow::Result;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::{info, warn};

use nmidi_protocol::{AppleMidiPacket, RtpPacket, APPLEMIDI_VERSION};

use crate::network::NetworkSockets;

#[derive(Debug, Clone)]
struct SessionState {
    ssrc: u32,
    token: u32,
    addr: SocketAddr,
    data_addr: SocketAddr,
    sequence: u16,
    timestamp: u32,
}

pub struct SessionManager {
    name: String,
    ssrc: u32,
    sockets: Arc<NetworkSockets>,
    sessions: Arc<Mutex<HashMap<u32, SessionState>>>,
}

impl SessionManager {
    pub async fn new(name: String, control_addr: String, data_addr: String) -> Result<Self> {
        let ssrc = generate_ssrc();
        let sockets = NetworkSockets::bind(&control_addr, &data_addr).await?;
        Ok(Self {
            name,
            ssrc,
            sockets: Arc::new(sockets),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn run(&self) -> Result<()> {
        let control_handle = {
            let sockets = Arc::clone(&self.sockets);
            let sessions = Arc::clone(&self.sessions);
            let name = self.name.clone();
            let ssrc = self.ssrc;

            tokio::spawn(async move {
                loop {
                    match sockets.recv_control().await {
                        Ok((packet, addr)) => {
                            if let Err(e) = Self::handle_control_packet(
                                &sockets,
                                &sessions,
                                &name,
                                ssrc,
                                packet,
                                addr,
                            )
                            .await
                            {
                                warn!("Error handling control packet: {}", e);
                            }
                        }
                        Err(e) => warn!("Error receiving control packet: {}", e),
                    }
                }
            })
        };

        let data_handle = {
            let sockets = Arc::clone(&self.sockets);
            let sessions = Arc::clone(&self.sessions);

            tokio::spawn(async move {
                loop {
                    match sockets.recv_data().await {
                        Ok((packet, addr)) => {
                            if let Err(e) =
                                Self::handle_data_packet(&sessions, packet, addr).await
                            {
                                warn!("Error handling data packet: {}", e);
                            }
                        }
                        Err(e) => warn!("Error receiving data packet: {}", e),
                    }
                }
            })
        };

        tokio::select! {
            _ = control_handle => {},
            _ = data_handle => {},
        }

        Ok(())
    }

    async fn handle_control_packet(
        sockets: &NetworkSockets,
        sessions: &Arc<Mutex<HashMap<u32, SessionState>>>,
        name: &str,
        ssrc: u32,
        packet: AppleMidiPacket,
        addr: SocketAddr,
    ) -> Result<()> {
        match packet {
            AppleMidiPacket::Invitation {
                version: _,
                token,
                ssrc: peer_ssrc,
                name: peer_name,
            } => {
                info!("Received invitation from {} ({})", peer_name, addr);

                // Send acceptance
                let response = AppleMidiPacket::InvitationAccepted {
                    version: APPLEMIDI_VERSION,
                    token,
                    ssrc,
                    name: name.to_string(),
                };
                sockets.send_control(&response, &addr).await?;

                // Store session
                let data_addr = SocketAddr::new(addr.ip(), addr.port() + 1);
                let mut sessions_lock = sessions.lock().await;
                sessions_lock.insert(
                    peer_ssrc,
                    SessionState {
                        ssrc: peer_ssrc,
                        token,
                        addr,
                        data_addr,
                        sequence: 0,
                        timestamp: 0,
                    },
                );

                info!("Session established with {}", peer_name);

                // Send initial synchronization
                let sync = AppleMidiPacket::Synchronization {
                    ssrc,
                    count: 0,
                    timestamp1: get_timestamp(),
                    timestamp2: 0,
                    timestamp3: 0,
                };
                sockets.send_control(&sync, &addr).await?;
            }
            AppleMidiPacket::InvitationAccepted {
                ssrc: peer_ssrc, ..
            } => {
                info!("Invitation accepted by peer SSRC {}", peer_ssrc);
            }
            AppleMidiPacket::End {
                ssrc: peer_ssrc, ..
            } => {
                info!("Session ended by peer SSRC {}", peer_ssrc);
                let mut sessions_lock = sessions.lock().await;
                sessions_lock.remove(&peer_ssrc);
            }
            AppleMidiPacket::Synchronization {
                ssrc: peer_ssrc,
                count,
                timestamp1,
                timestamp2,
                ..
            } => {
                info!("Received sync from SSRC {} (count {})", peer_ssrc, count);

                // Respond to sync
                let sessions_lock = sessions.lock().await;
                if let Some(session) = sessions_lock.get(&peer_ssrc) {
                    let response = AppleMidiPacket::Synchronization {
                        ssrc,
                        count: count + 1,
                        timestamp1,
                        timestamp2,
                        timestamp3: get_timestamp(),
                    };
                    sockets.send_control(&response, &session.addr).await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_data_packet(
        _sessions: &Arc<Mutex<HashMap<u32, SessionState>>>,
        packet: RtpPacket,
        _addr: SocketAddr,
    ) -> Result<()> {
        // Process MIDI commands
        for cmd in &packet.commands {
            info!(
                "MIDI command: delta={}, data={:?}",
                cmd.delta_time, cmd.data
            );
            // TODO: Forward to MIDI output port
        }

        Ok(())
    }
}

fn generate_ssrc() -> u32 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    (now & 0xFFFFFFFF) as u32
}

fn get_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}
