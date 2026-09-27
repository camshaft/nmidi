//! Local MIDI port enumeration, moved to `nmidi-core` so the server and the
//! `nmidid` control-socket daemon share one implementation. Re-exported here so
//! existing `crate::midi::…` paths keep resolving.

pub use nmidi_core::midi::*;
