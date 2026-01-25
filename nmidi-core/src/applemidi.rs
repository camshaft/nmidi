use crate::{APPLEMIDI_SIGNATURE, error::ProtocolError};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use std::io::{Cursor, Read};

/// AppleMIDI protocol commands (4 bytes)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppleMidiCommand {
    /// Invitation to connect ("IN")
    Invitation,
    /// Invitation accepted ("OK")
    InvitationAccepted,
    /// End session ("BY")
    End,
    /// Clock synchronization ("CK")
    Synchronization,
    /// Receiver feedback ("RS")
    ReceiverFeedback,
}

impl AppleMidiCommand {
    pub fn as_bytes(&self) -> [u8; 4] {
        match self {
            Self::Invitation => *b"IN  ",
            Self::InvitationAccepted => *b"OK  ",
            Self::End => *b"BY  ",
            Self::Synchronization => *b"CK  ",
            Self::ReceiverFeedback => *b"RS  ",
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if bytes.len() < 4 {
            return Err(ProtocolError::InvalidCommand(
                "Command must be 4 bytes".to_string(),
            ));
        }

        match &bytes[0..4] {
            b"IN  " => Ok(Self::Invitation),
            b"OK  " => Ok(Self::InvitationAccepted),
            b"BY  " => Ok(Self::End),
            b"CK  " => Ok(Self::Synchronization),
            b"RS  " => Ok(Self::ReceiverFeedback),
            _ => Err(ProtocolError::InvalidCommand(format!(
                "Unknown command: {:?}",
                &bytes[0..4]
            ))),
        }
    }
}

/// AppleMIDI protocol packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppleMidiPacket {
    /// Invitation packet
    Invitation {
        version: u32,
        token: u32,
        ssrc: u32,
        name: String,
    },
    /// Invitation accepted packet
    InvitationAccepted {
        version: u32,
        token: u32,
        ssrc: u32,
        name: String,
    },
    /// End session packet
    End { version: u32, token: u32, ssrc: u32 },
    /// Synchronization packet
    Synchronization {
        ssrc: u32,
        count: u8,
        timestamp1: u64,
        timestamp2: u64,
        timestamp3: u64,
    },
}

impl AppleMidiPacket {
    /// Parse an AppleMIDI packet from bytes
    pub fn parse(data: &[u8]) -> Result<Self, ProtocolError> {
        if data.len() < 6 {
            return Err(ProtocolError::PacketTooShort {
                expected: 6,
                got: data.len(),
            });
        }

        let mut cursor = Cursor::new(data);
        let signature = cursor.read_u16::<BigEndian>()?;

        if signature != APPLEMIDI_SIGNATURE {
            return Err(ProtocolError::InvalidSignature {
                expected: APPLEMIDI_SIGNATURE,
                got: signature,
            });
        }

        // Read command bytes using cursor
        let mut cmd_bytes = [0u8; 4];
        cursor.read_exact(&mut cmd_bytes)?;
        let command = AppleMidiCommand::from_bytes(&cmd_bytes)?;

        match command {
            AppleMidiCommand::Invitation | AppleMidiCommand::InvitationAccepted => {
                if data.len() < 18 {
                    return Err(ProtocolError::PacketTooShort {
                        expected: 18,
                        got: data.len(),
                    });
                }

                let version = cursor.read_u32::<BigEndian>()?;
                let token = cursor.read_u32::<BigEndian>()?;
                let ssrc = cursor.read_u32::<BigEndian>()?;

                // Read name as null-terminated string
                let name_bytes = &data[18..];
                let name_end = name_bytes
                    .iter()
                    .position(|&b| b == 0)
                    .unwrap_or(name_bytes.len());
                let name = String::from_utf8_lossy(&name_bytes[..name_end]).to_string();

                if command == AppleMidiCommand::Invitation {
                    Ok(AppleMidiPacket::Invitation {
                        version,
                        token,
                        ssrc,
                        name,
                    })
                } else {
                    Ok(AppleMidiPacket::InvitationAccepted {
                        version,
                        token,
                        ssrc,
                        name,
                    })
                }
            }
            AppleMidiCommand::End => {
                if data.len() < 18 {
                    return Err(ProtocolError::PacketTooShort {
                        expected: 18,
                        got: data.len(),
                    });
                }

                let version = cursor.read_u32::<BigEndian>()?;
                let token = cursor.read_u32::<BigEndian>()?;
                let ssrc = cursor.read_u32::<BigEndian>()?;

                Ok(AppleMidiPacket::End {
                    version,
                    token,
                    ssrc,
                })
            }
            AppleMidiCommand::Synchronization => {
                if data.len() < 35 {
                    return Err(ProtocolError::PacketTooShort {
                        expected: 35,
                        got: data.len(),
                    });
                }

                let ssrc = cursor.read_u32::<BigEndian>()?;
                let count = cursor.read_u8()?;
                cursor.read_u8()?; // padding
                cursor.read_u8()?; // padding
                cursor.read_u8()?; // padding
                let timestamp1 = cursor.read_u64::<BigEndian>()?;
                let timestamp2 = cursor.read_u64::<BigEndian>()?;
                let timestamp3 = cursor.read_u64::<BigEndian>()?;

                Ok(AppleMidiPacket::Synchronization {
                    ssrc,
                    count,
                    timestamp1,
                    timestamp2,
                    timestamp3,
                })
            }
            AppleMidiCommand::ReceiverFeedback => Err(ProtocolError::InvalidCommand(
                "ReceiverFeedback not yet implemented".to_string(),
            )),
        }
    }

    /// Serialize the packet to bytes
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_u16(APPLEMIDI_SIGNATURE);

        match self {
            AppleMidiPacket::Invitation {
                version,
                token,
                ssrc,
                name,
            } => {
                buf.put_slice(&AppleMidiCommand::Invitation.as_bytes());
                buf.put_u32(*version);
                buf.put_u32(*token);
                buf.put_u32(*ssrc);
                buf.put_slice(name.as_bytes());
                buf.put_u8(0); // null terminator
            }
            AppleMidiPacket::InvitationAccepted {
                version,
                token,
                ssrc,
                name,
            } => {
                buf.put_slice(&AppleMidiCommand::InvitationAccepted.as_bytes());
                buf.put_u32(*version);
                buf.put_u32(*token);
                buf.put_u32(*ssrc);
                buf.put_slice(name.as_bytes());
                buf.put_u8(0); // null terminator
            }
            AppleMidiPacket::End {
                version,
                token,
                ssrc,
            } => {
                buf.put_slice(&AppleMidiCommand::End.as_bytes());
                buf.put_u32(*version);
                buf.put_u32(*token);
                buf.put_u32(*ssrc);
            }
            AppleMidiPacket::Synchronization {
                ssrc,
                count,
                timestamp1,
                timestamp2,
                timestamp3,
            } => {
                buf.put_slice(&AppleMidiCommand::Synchronization.as_bytes());
                buf.put_u32(*ssrc);
                buf.put_u8(*count);
                buf.put_u8(0); // padding
                buf.put_u8(0); // padding
                buf.put_u8(0); // padding
                buf.put_u64(*timestamp1);
                buf.put_u64(*timestamp2);
                buf.put_u64(*timestamp3);
            }
        }

        buf.freeze()
    }

    pub fn ssrc(&self) -> u32 {
        match self {
            AppleMidiPacket::Invitation { ssrc, .. }
            | AppleMidiPacket::InvitationAccepted { ssrc, .. }
            | AppleMidiPacket::End { ssrc, .. }
            | AppleMidiPacket::Synchronization { ssrc, .. } => *ssrc,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invitation_roundtrip() {
        let packet = AppleMidiPacket::Invitation {
            version: 2,
            token: 12345,
            ssrc: 67890,
            name: "TestDevice".to_string(),
        };

        let bytes = packet.to_bytes();
        let parsed = AppleMidiPacket::parse(&bytes).unwrap();

        assert_eq!(packet, parsed);
    }

    #[test]
    fn test_end_roundtrip() {
        let packet = AppleMidiPacket::End {
            version: 2,
            token: 12345,
            ssrc: 67890,
        };

        let bytes = packet.to_bytes();
        let parsed = AppleMidiPacket::parse(&bytes).unwrap();

        assert_eq!(packet, parsed);
    }

    #[test]
    fn test_sync_roundtrip() {
        let packet = AppleMidiPacket::Synchronization {
            ssrc: 67890,
            count: 0,
            timestamp1: 1000,
            timestamp2: 2000,
            timestamp3: 3000,
        };

        let bytes = packet.to_bytes();
        let parsed = AppleMidiPacket::parse(&bytes).unwrap();

        assert_eq!(packet, parsed);
    }
}
