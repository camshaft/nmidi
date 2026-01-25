use anyhow::{Context, Result};
use mdns_sd::{ServiceDaemon, ServiceInfo};
use std::collections::HashMap;

use crate::midi::MidiPorts;

const SERVICE_TYPE: &str = "_apple-midi._udp.local.";

/// Advertise MIDI service via mDNS
pub fn advertise_service(
    device_name: &str,
    control_port: u16,
) -> Result<ServiceDaemon> {
    let mdns = ServiceDaemon::new().context("Failed to create mDNS daemon")?;

    // Create TXT records
    let mut properties = HashMap::new();
    properties.insert("name".to_string(), device_name.to_string());
    properties.insert("ver".to_string(), "2".to_string());

    // Register service
    let service_info = ServiceInfo::new(
        SERVICE_TYPE,
        device_name,
        device_name,
        "",
        control_port,
        Some(properties),
    )
    .context("Failed to create service info")?;

    mdns.register(service_info)
        .context("Failed to register mDNS service")?;

    Ok(mdns)
}

/// Browse for MIDI services on the network
pub fn browse_services() -> Result<mdns_sd::Receiver<mdns_sd::ServiceEvent>> {
    let mdns = ServiceDaemon::new().context("Failed to create mDNS daemon")?;
    let receiver = mdns.browse(SERVICE_TYPE).context("Failed to browse for services")?;
    Ok(receiver)
}
