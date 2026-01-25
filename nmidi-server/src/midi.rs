use anyhow::{Context, Result};
use midir::{MidiInput, MidiOutput};

#[derive(Debug, Clone)]
pub struct MidiPortInfo {
    pub name: String,
    pub index: usize,
}

#[derive(Debug, Clone)]
pub struct MidiPorts {
    pub inputs: Vec<MidiPortInfo>,
    pub outputs: Vec<MidiPortInfo>,
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
            })
        })
        .collect();

    Ok(MidiPorts { inputs, outputs })
}
