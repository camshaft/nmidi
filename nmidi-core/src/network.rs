use anyhow::{Context, Result};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tracing::{debug, warn};

use crate::{AppleMidiPacket, RtpPacket};

/// Maximum UDP payload size for MIDI packets (MTU - IP header - UDP header)
const MAX_UDP_PAYLOAD: usize = 1500;

pub struct NetworkSockets {
    pub control: UdpSocket,
    pub data: UdpSocket,
}

impl NetworkSockets {
    pub async fn bind(control_addr: &str, data_addr: &str) -> Result<Self> {
        let control = UdpSocket::bind(control_addr)
            .await
            .context("Failed to bind control socket")?;

        let data = UdpSocket::bind(data_addr)
            .await
            .context("Failed to bind data socket")?;

        debug!("Control socket bound to: {}", control.local_addr()?);
        debug!("Data socket bound to: {}", data.local_addr()?);

        Ok(Self { control, data })
    }

    pub async fn recv_control(&self) -> Result<(AppleMidiPacket, SocketAddr)> {
        let mut buf = [0u8; MAX_UDP_PAYLOAD];
        let (len, addr) = self.control.recv_from(&mut buf).await?;

        match AppleMidiPacket::parse(&buf[..len]) {
            Ok(packet) => {
                debug!("Received control packet from {}: {:?}", addr, packet);
                Ok((packet, addr))
            }
            Err(e) => {
                warn!("Failed to parse control packet from {}: {}", addr, e);
                Err(e.into())
            }
        }
    }

    pub async fn send_control(
        &self,
        packet: &AppleMidiPacket,
        addr: &SocketAddr,
    ) -> Result<()> {
        let bytes = packet.to_bytes();
        self.control.send_to(&bytes, addr).await?;
        debug!("Sent control packet to {}: {:?}", addr, packet);
        Ok(())
    }

    pub async fn recv_data(&self) -> Result<(RtpPacket, SocketAddr)> {
        let mut buf = [0u8; MAX_UDP_PAYLOAD];
        let (len, addr) = self.data.recv_from(&mut buf).await?;

        match RtpPacket::parse(&buf[..len]) {
            Ok(packet) => {
                debug!("Received data packet from {}", addr);
                Ok((packet, addr))
            }
            Err(e) => {
                warn!("Failed to parse data packet from {}: {}", addr, e);
                Err(e.into())
            }
        }
    }

    pub async fn send_data(&self, packet: &RtpPacket, addr: &SocketAddr) -> Result<()> {
        let bytes = packet.to_bytes();
        self.data.send_to(&bytes, addr).await?;
        debug!("Sent data packet to {}", addr);
        Ok(())
    }
}
