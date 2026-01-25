pub mod applemidi;
pub mod rtp;
pub mod error;

pub use applemidi::{AppleMidiCommand, AppleMidiPacket};
pub use rtp::{RtpPacket, MidiCommand};
pub use error::ProtocolError;

pub const APPLEMIDI_SIGNATURE: u16 = 0xFFFF;
pub const APPLEMIDI_VERSION: u32 = 2;
