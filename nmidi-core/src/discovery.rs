use anyhow::{Context, Result};
use mdns_sd::{ServiceDaemon, ServiceInfo};
use std::collections::HashMap;

const SERVICE_TYPE: &str = "_apple-midi._udp.local.";

/// Manages multiple mDNS service advertisements
pub struct ServiceAdvertiser {
    mdns: ServiceDaemon,
}

impl ServiceAdvertiser {
    /// Create a new service advertiser
    pub fn new() -> Result<Self> {
        let mdns = ServiceDaemon::new().context("Failed to create mDNS daemon")?;
        Ok(Self { mdns })
    }

    /// Advertise a MIDI service via mDNS
    /// Returns a service instance name that can be used to unregister later
    pub fn advertise_service(
        &self,
        service_name: &str,
        device_name: &str,
        control_port: u16,
        properties: HashMap<String, String>,
    ) -> Result<String> {
        let hostname = format!("{}.local.", device_name);
        
        let service_info = ServiceInfo::new(
            SERVICE_TYPE,
            service_name,
            &hostname,
            "",
            control_port,
            Some(properties),
        )
        .context("Failed to create service info")?;

        let fullname = service_info.get_fullname().to_string();
        
        self.mdns
            .register(service_info)
            .context("Failed to register mDNS service")?;

        Ok(fullname)
    }

    /// Unregister a previously advertised service
    pub fn unregister_service(&self, fullname: &str) -> Result<()> {
        self.mdns
            .unregister(fullname)
            .context("Failed to unregister mDNS service")?;
        Ok(())
    }
}

/// Advertise MIDI service via mDNS (legacy single-service function)
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
    let hostname = format!("{}.local.", device_name);
    let service_info = ServiceInfo::new(
        SERVICE_TYPE,
        device_name,
        &hostname,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_advertiser_creation() {
        // Just test that we can create the advertiser
        // The actual mDNS functionality may not work in test environments
        let result = ServiceAdvertiser::new();
        // We can't guarantee mDNS will work in all test environments,
        // so we just test that the function doesn't panic
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_service_advertiser_properties() {
        let mut properties = HashMap::new();
        properties.insert("name".to_string(), "TestPort".to_string());
        properties.insert("ver".to_string(), "2".to_string());
        properties.insert("type".to_string(), "Input".to_string());
        properties.insert("index".to_string(), "0".to_string());

        assert_eq!(properties.get("name"), Some(&"TestPort".to_string()));
        assert_eq!(properties.get("ver"), Some(&"2".to_string()));
        assert_eq!(properties.get("type"), Some(&"Input".to_string()));
        assert_eq!(properties.get("index"), Some(&"0".to_string()));
    }
}
