# nmidi

MIDI Over Network Daemon - RTP-MIDI implementation with client and server

## Overview

`nmidi` is a Rust-based implementation of the RTP-MIDI (AppleMIDI) protocol that enables MIDI communication over IP networks. It consists of three main components:

- **nmidi-protocol**: Core protocol library implementing AppleMIDI session management and RTP-MIDI packet encoding/decoding
- **nmidi-server**: Network MIDI server that exposes local MIDI ports over the network
- **nmidi-client**: Network MIDI client that connects to remote MIDI services

This implementation follows the specifications in:
- RFC 6295: RTP Payload Format for MIDI
- Apple MIDI Network Driver Protocol
- RFC 6762/6763: DNS-SD over mDNS for service discovery

## Features

- ✅ AppleMIDI protocol support (session handshake, synchronization)
- ✅ RTP-MIDI packet encoding/decoding
- ✅ mDNS-based service discovery (`_apple-midi._udp`)
- ✅ UDP-based transport (control and data channels)
- ✅ Automatic MIDI port detection
- ✅ Compatible with macOS Audio MIDI Setup
- ✅ Cross-platform (Linux, macOS, Windows with ALSA/CoreMIDI/WinMM)

## Building

### Prerequisites

On Linux (Debian/Ubuntu):
```bash
sudo apt-get install libasound2-dev
```

On macOS:
```bash
# CoreMIDI is included with Xcode
```

### Compile

```bash
cargo build --release
```

## Usage

### Server

Start a server to expose local MIDI ports:

```bash
./target/release/nmidi-server --name "My MIDI Server"
```

Options:
- `-n, --name <NAME>`: Device name to advertise (default: "nmidi-server")
- `-p, --control-port <PORT>`: Control port (default: 5004, data port will be +1)
- `-b, --bind <ADDR>`: Bind address (default: "0.0.0.0")
- `-l, --log-level <LEVEL>`: Log level (trace, debug, info, warn, error)

The server will:
1. Detect available MIDI input and output ports
2. Advertise itself via mDNS with service type `_apple-midi._udp`
3. Listen for incoming connections on the control and data ports
4. Handle AppleMIDI session handshakes and MIDI data transport

### Client

Connect to a remote MIDI server:

```bash
./target/release/nmidi-client --host 192.168.1.100 --port 5004
```

Or browse for available services:

```bash
./target/release/nmidi-client --browse
```

Options:
- `-H, --host <HOST>`: Remote host to connect to
- `-p, --port <PORT>`: Remote control port (default: 5004)
- `-n, --name <NAME>`: Device name (default: "nmidi-client")
- `-b, --bind <ADDR>`: Local bind address (default: "0.0.0.0:0")
- `-B, --browse`: Browse for services instead of connecting directly
- `-l, --log-level <LEVEL>`: Log level

## Protocol Implementation

### AppleMIDI Protocol

The implementation includes:

- **Session Management**:
  - `IN` (Invitation): Initiate connection
  - `OK` (Invitation Accepted): Accept connection
  - `BY` (End): Terminate session
  - `CK` (Synchronization): Clock sync for timestamp alignment

- **Packet Structure**:
  - 2-byte signature (0xFFFF)
  - 4-byte command
  - Version, token, SSRC fields
  - Device name (null-terminated string)

### RTP-MIDI Protocol

- **RTP Header**: Standard RTP v2 header with MIDI payload type
- **MIDI Command Section**: Timestamped MIDI events with delta times
- **Variable-Length Encoding**: For delta times and command lengths

### Service Discovery

Uses DNS-SD over mDNS:
- Service type: `_apple-midi._udp.local.`
- TXT records: `name=<device>`, `ver=2`
- Automatically discoverable by macOS Audio MIDI Setup

## Testing

Run the protocol tests:

```bash
cargo test --package nmidi-protocol
```

### Manual Testing

1. Start the server on one machine:
   ```bash
   ./target/release/nmidi-server --name "TestServer"
   ```

2. From another machine (or terminal), browse for services:
   ```bash
   ./target/release/nmidi-client --browse
   ```

3. Connect to the server:
   ```bash
   ./target/release/nmidi-client --host <server-ip> --port 5004
   ```

4. On macOS, open "Audio MIDI Setup" → "MIDI Studio" → "Network" to see the advertised service

## Architecture

```
┌─────────────────────┐         ┌─────────────────────┐
│   nmidi-server      │         │   nmidi-client      │
├─────────────────────┤         ├─────────────────────┤
│ - MIDI Detection    │         │ - Service Browser   │
│ - mDNS Advertiser   │◄───────►│ - Connection Mgmt   │
│ - Session Manager   │         │ - MIDI I/O          │
│ - UDP Sockets       │         │ - UDP Sockets       │
└──────┬──────────────┘         └──────┬──────────────┘
       │                               │
       │    nmidi-protocol (shared)    │
       │  ┌───────────────────────┐    │
       └─►│ - AppleMIDI Protocol  │◄───┘
          │ - RTP-MIDI Packets    │
          │ - Packet Encoding     │
          └───────────────────────┘
```

## Project Structure

```
nmidi/
├── Cargo.toml              # Workspace configuration
├── nmidi-protocol/         # Core protocol library
│   ├── src/
│   │   ├── lib.rs
│   │   ├── applemidi.rs    # AppleMIDI protocol
│   │   ├── rtp.rs          # RTP-MIDI protocol
│   │   └── error.rs        # Error types
│   └── Cargo.toml
├── nmidi-server/           # Server binary
│   ├── src/
│   │   ├── main.rs
│   │   ├── discovery.rs    # mDNS service advertisement
│   │   ├── midi.rs         # MIDI port detection
│   │   ├── network.rs      # UDP socket management
│   │   └── session.rs      # Session state management
│   └── Cargo.toml
└── nmidi-client/           # Client binary
    ├── src/
    │   ├── main.rs
    │   ├── discovery.rs    # mDNS service browsing
    │   └── network.rs      # UDP socket management
    └── Cargo.toml
```

## Dependencies

- **tokio**: Async runtime
- **midir**: Cross-platform MIDI I/O
- **mdns-sd**: mDNS service discovery
- **bytes**: Efficient byte buffer management
- **byteorder**: Endian-aware binary encoding
- **clap**: Command-line argument parsing
- **tracing**: Structured logging

## Future Enhancements

- [ ] Recovery journal implementation for packet loss handling
- [ ] Support for multiple simultaneous sessions
- [ ] MIDI port selection via command-line
- [ ] Bidirectional MIDI routing (network → local output)
- [ ] Latency monitoring and reporting
- [ ] Configuration file support
- [ ] systemd service files for Linux

## License

MIT

## References

- [RFC 6295 - RTP Payload Format for MIDI](https://www.rfc-editor.org/rfc/rfc6295.html)
- [Apple MIDI Network Driver Protocol](https://developer.apple.com/library/archive/documentation/Audio/Conceptual/MIDINetworkDriverProtocol/)
- [RFC 6762 - Multicast DNS](https://datatracker.ietf.org/doc/html/rfc6762)
- [RFC 6763 - DNS-Based Service Discovery](https://datatracker.ietf.org/doc/html/rfc6763)
