use bytes::{BufMut, Bytes, BytesMut};
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;

use crate::error::ProtocolError;

/// RTP packet header
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtpHeader {
    pub version: u8,
    pub padding: bool,
    pub extension: bool,
    pub csrc_count: u8,
    pub marker: bool,
    pub payload_type: u8,
    pub sequence: u16,
    pub timestamp: u32,
    pub ssrc: u32,
}

impl Default for RtpHeader {
    fn default() -> Self {
        Self {
            version: 2,
            padding: false,
            extension: false,
            csrc_count: 0,
            marker: false,
            payload_type: 97, // Dynamic payload type for MIDI
            sequence: 0,
            timestamp: 0,
            ssrc: 0,
        }
    }
}

impl RtpHeader {
    pub fn parse(data: &[u8]) -> Result<(Self, usize), ProtocolError> {
        if data.len() < 12 {
            return Err(ProtocolError::PacketTooShort {
                expected: 12,
                got: data.len(),
            });
        }

        let mut cursor = Cursor::new(data);

        let byte0 = cursor.read_u8()?;
        let version = (byte0 >> 6) & 0x03;
        let padding = (byte0 >> 5) & 0x01 != 0;
        let extension = (byte0 >> 4) & 0x01 != 0;
        let csrc_count = byte0 & 0x0F;

        let byte1 = cursor.read_u8()?;
        let marker = (byte1 >> 7) & 0x01 != 0;
        let payload_type = byte1 & 0x7F;

        let sequence = cursor.read_u16::<BigEndian>()?;
        let timestamp = cursor.read_u32::<BigEndian>()?;
        let ssrc = cursor.read_u32::<BigEndian>()?;

        let header_size = 12 + (csrc_count as usize * 4);

        if data.len() < header_size {
            return Err(ProtocolError::PacketTooShort {
                expected: header_size,
                got: data.len(),
            });
        }

        Ok((
            RtpHeader {
                version,
                padding,
                extension,
                csrc_count,
                marker,
                payload_type,
                sequence,
                timestamp,
                ssrc,
            },
            header_size,
        ))
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        let byte0 = (self.version << 6)
            | ((self.padding as u8) << 5)
            | ((self.extension as u8) << 4)
            | self.csrc_count;

        let byte1 = ((self.marker as u8) << 7) | self.payload_type;

        buf.put_u8(byte0);
        buf.put_u8(byte1);
        buf.put_u16(self.sequence);
        buf.put_u32(self.timestamp);
        buf.put_u32(self.ssrc);

        buf.freeze()
    }
}

/// MIDI command in RTP payload
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MidiCommand {
    pub delta_time: u32,
    pub data: Vec<u8>,
}

/// RTP-MIDI packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtpPacket {
    pub header: RtpHeader,
    pub commands: Vec<MidiCommand>,
}

impl RtpPacket {
    pub fn new(ssrc: u32, sequence: u16, timestamp: u32) -> Self {
        Self {
            header: RtpHeader {
                ssrc,
                sequence,
                timestamp,
                ..Default::default()
            },
            commands: Vec::new(),
        }
    }

    pub fn add_command(&mut self, delta_time: u32, data: Vec<u8>) {
        self.commands.push(MidiCommand { delta_time, data });
    }

    /// Parse an RTP-MIDI packet
    pub fn parse(data: &[u8]) -> Result<Self, ProtocolError> {
        let (header, header_size) = RtpHeader::parse(data)?;

        if data.len() < header_size + 1 {
            return Err(ProtocolError::PacketTooShort {
                expected: header_size + 1,
                got: data.len(),
            });
        }

        let payload = &data[header_size..];
        let commands = Self::parse_midi_commands(payload)?;

        Ok(RtpPacket { header, commands })
    }

    /// Parse MIDI command section
    fn parse_midi_commands(data: &[u8]) -> Result<Vec<MidiCommand>, ProtocolError> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let mut commands = Vec::new();
        let mut cursor = Cursor::new(data);

        // Skip flags byte for now (B, J, Z, P flags)
        let flags = cursor.read_u8()?;
        let has_journal = (flags >> 7) & 0x01 != 0;

        // Read length if present (when B flag is set)
        let b_flag = (flags >> 7) & 0x01 != 0;
        if !b_flag {
            // Short header, len is lower 4 bits of flags
            let _len = flags & 0x0F;
        } else {
            // Long header, next byte is length
            if cursor.position() >= data.len() as u64 {
                return Ok(commands);
            }
            let _len = cursor.read_u8()?;
        }

        // Parse MIDI commands
        while cursor.position() < data.len() as u64 {
            // Check if we've reached the journal section
            if has_journal && cursor.position() > data.len() as u64 / 2 {
                // Simple heuristic to stop before journal
                break;
            }

            // Read delta time (variable length)
            let delta_time = match Self::read_variable_length(&mut cursor) {
                Ok(val) => val,
                Err(_) => break,
            };

            // Read MIDI status byte
            let status = match cursor.read_u8() {
                Ok(b) => b,
                Err(_) => break,
            };

            // Determine command length based on status
            let mut midi_data = vec![status];

            if status >= 0x80 {
                // Status byte present
                let data_len = match status & 0xF0 {
                    0x80 | 0x90 | 0xA0 | 0xB0 | 0xE0 => 2, // Note Off, Note On, etc. - 2 data bytes
                    0xC0 | 0xD0 => 1,                       // Program Change, Channel Pressure - 1 data byte
                    0xF0 => {
                        // System messages
                        match status {
                            0xF1 | 0xF3 => 1, // MTC Quarter Frame, Song Select
                            0xF2 => 2,        // Song Position Pointer
                            _ => 0,           // Other system messages
                        }
                    }
                    _ => 0,
                };

                for _ in 0..data_len {
                    match cursor.read_u8() {
                        Ok(b) => midi_data.push(b),
                        Err(_) => break,
                    }
                }
            }

            commands.push(MidiCommand {
                delta_time,
                data: midi_data,
            });

            // Simple check to prevent infinite loops
            if commands.len() > 100 {
                break;
            }
        }

        Ok(commands)
    }

    /// Read variable-length quantity
    fn read_variable_length(cursor: &mut Cursor<&[u8]>) -> Result<u32, std::io::Error> {
        let mut value = 0u32;
        let mut byte;

        loop {
            byte = cursor.read_u8()?;
            value = (value << 7) | ((byte & 0x7F) as u32);

            if (byte & 0x80) == 0 {
                break;
            }
        }

        Ok(value)
    }

    /// Write variable-length quantity
    fn write_variable_length(buf: &mut BytesMut, mut value: u32) {
        let mut bytes = Vec::new();

        bytes.push((value & 0x7F) as u8);
        value >>= 7;

        while value > 0 {
            bytes.push(((value & 0x7F) | 0x80) as u8);
            value >>= 7;
        }

        for byte in bytes.iter().rev() {
            buf.put_u8(*byte);
        }
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Write RTP header
        buf.put(self.header.to_bytes());

        // Write MIDI command section
        let mut payload = BytesMut::new();

        // Flags byte: B flag (no journal for now)
        let has_journal = false;
        let b_flag = self.commands.len() > 0;

        if !b_flag {
            payload.put_u8(0); // No commands
        } else {
            // For simplicity, use short header when possible
            if self.commands.len() <= 15 {
                let flags = (has_journal as u8) << 7 | (self.commands.len() as u8);
                payload.put_u8(flags);
            } else {
                let flags = 0x80 | ((has_journal as u8) << 7);
                payload.put_u8(flags);
                payload.put_u8(self.commands.len() as u8);
            }

            // Write MIDI commands
            for cmd in &self.commands {
                Self::write_variable_length(&mut payload, cmd.delta_time);
                payload.put_slice(&cmd.data);
            }
        }

        buf.put(payload.freeze());
        buf.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rtp_header_roundtrip() {
        let header = RtpHeader {
            version: 2,
            padding: false,
            extension: false,
            csrc_count: 0,
            marker: true,
            payload_type: 97,
            sequence: 1234,
            timestamp: 5678,
            ssrc: 9012,
        };

        let bytes = header.to_bytes();
        let (parsed, size) = RtpHeader::parse(&bytes).unwrap();

        assert_eq!(size, 12);
        assert_eq!(header, parsed);
    }

    #[test]
    fn test_rtp_packet_roundtrip() {
        let mut packet = RtpPacket::new(12345, 100, 1000);
        packet.add_command(0, vec![0x90, 0x3C, 0x64]); // Note On

        let bytes = packet.to_bytes();
        let parsed = RtpPacket::parse(&bytes).unwrap();

        assert_eq!(packet.header.ssrc, parsed.header.ssrc);
        assert_eq!(packet.header.sequence, parsed.header.sequence);
        assert_eq!(packet.commands.len(), parsed.commands.len());
    }
}
