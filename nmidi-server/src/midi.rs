use anyhow::{Context, Result};
use midir::{MidiInput, MidiOutput};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MidiPortInfo {
    pub name: String,
    pub index: usize,
    pub port_type: MidiPortType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MidiPortType {
    Input,
    Output,
}

impl MidiPortType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MidiPortType::Input => "input",
            MidiPortType::Output => "output",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MidiPorts {
    pub inputs: Vec<MidiPortInfo>,
    pub outputs: Vec<MidiPortInfo>,
}

impl MidiPorts {
    pub fn all_ports(&self) -> Vec<MidiPortInfo> {
        self.inputs.iter().chain(self.outputs.iter()).cloned().collect()
    }
}

/// Detect available MIDI ports
pub fn detect_ports() -> Result<MidiPorts> {
    let midi_in = MidiInput::new("nmidi-server-detect")
        .context("Failed to create MIDI input")?;
    let midi_out = MidiOutput::new("nmidi-server-detect")
        .context("Failed to create MIDI output")?;

    let input_ports = midi_in.ports();
    let output_ports = midi_out.ports();

    let inputs = input_ports
        .iter()
        .enumerate()
        .filter_map(|(idx, port)| {
            midi_in.port_name(port).ok().map(|name| MidiPortInfo {
                name,
                index: idx,
                port_type: MidiPortType::Input,
            })
        })
        .collect();

    let outputs = output_ports
        .iter()
        .enumerate()
        .filter_map(|(idx, port)| {
            midi_out.port_name(port).ok().map(|name| MidiPortInfo {
                name,
                index: idx,
                port_type: MidiPortType::Output,
            })
        })
        .collect();

    Ok(MidiPorts { inputs, outputs })
}

/// Start monitoring MIDI ports for changes
/// Returns a watch receiver that will be notified whenever ports change
pub async fn start_port_monitor(poll_interval: Duration) -> watch::Receiver<MidiPorts> {
    let (tx, rx) = watch::channel(MidiPorts {
        inputs: Vec::new(),
        outputs: Vec::new(),
    });

    tokio::spawn(async move {
        let mut last_ports = MidiPorts {
            inputs: Vec::new(),
            outputs: Vec::new(),
        };

        loop {
            tokio::time::sleep(poll_interval).await;

            match detect_ports() {
                Ok(current_ports) => {
                    if current_ports != last_ports {
                        info!(
                            "MIDI port change detected: {} inputs, {} outputs",
                            current_ports.inputs.len(),
                            current_ports.outputs.len()
                        );
                        debug!("New ports: {:?}", current_ports);
                        last_ports = current_ports.clone();
                        if tx.send(current_ports).is_err() {
                            // Receiver dropped, exit monitoring
                            break;
                        }
                    }
                }
                Err(e) => {
                    debug!("Error detecting MIDI ports: {}", e);
                }
            }
        }
    });

    rx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_midi_port_info_creation() {
        let port = MidiPortInfo {
            name: "Test Port".to_string(),
            index: 0,
            port_type: MidiPortType::Input,
        };
        assert_eq!(port.name, "Test Port");
        assert_eq!(port.index, 0);
        assert_eq!(port.port_type, MidiPortType::Input);
    }

    #[test]
    fn test_midi_ports_all_ports() {
        let ports = MidiPorts {
            inputs: vec![
                MidiPortInfo {
                    name: "Input 1".to_string(),
                    index: 0,
                    port_type: MidiPortType::Input,
                },
                MidiPortInfo {
                    name: "Input 2".to_string(),
                    index: 1,
                    port_type: MidiPortType::Input,
                },
            ],
            outputs: vec![MidiPortInfo {
                name: "Output 1".to_string(),
                index: 0,
                port_type: MidiPortType::Output,
            }],
        };

        let all = ports.all_ports();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].port_type, MidiPortType::Input);
        assert_eq!(all[1].port_type, MidiPortType::Input);
        assert_eq!(all[2].port_type, MidiPortType::Output);
    }

    #[test]
    fn test_midi_ports_equality() {
        let ports1 = MidiPorts {
            inputs: vec![MidiPortInfo {
                name: "Test".to_string(),
                index: 0,
                port_type: MidiPortType::Input,
            }],
            outputs: vec![],
        };

        let ports2 = MidiPorts {
            inputs: vec![MidiPortInfo {
                name: "Test".to_string(),
                index: 0,
                port_type: MidiPortType::Input,
            }],
            outputs: vec![],
        };

        let ports3 = MidiPorts {
            inputs: vec![MidiPortInfo {
                name: "Different".to_string(),
                index: 0,
                port_type: MidiPortType::Input,
            }],
            outputs: vec![],
        };

        assert_eq!(ports1, ports2);
        assert_ne!(ports1, ports3);
    }
}
