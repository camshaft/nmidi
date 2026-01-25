# nmidi

MIDI Over Network Daemon - RTP-MIDI implementation with client and server

## Overview

`nmidi` is a Rust-based implementation of the RTP-MIDI (AppleMIDI) protocol that enables MIDI communication over IP networks. It consists of two main components:

- **nmidi-core**: Core library implementing AppleMIDI session management, RTP-MIDI packet encoding/decoding, network sockets, and mDNS discovery
- **nmidi-server**: Network MIDI server that exposes local MIDI ports over the network
- **nmidi-client**: Network MIDI client that connects to remote MIDI services

This implementation follows the specifications in:
- RFC 6295: RTP Payload Format for MIDI
- Apple MIDI Network Driver Protocol
- RFC 6762/6763: DNS-SD over mDNS for service discovery

## Features

- AppleMIDI protocol support (session handshake, synchronization)
- RTP-MIDI packet encoding/decoding
- mDNS-based service discovery (`_apple-midi._udp`)
- UDP-based transport (control and data channels)
- Automatic MIDI port detection with continuous monitoring
- **Multi-port advertising**: Each MIDI port gets its own mDNS entry (Apple MIDI compatible)
- Compatible with macOS Audio MIDI Setup
- Cross-platform (Linux, macOS, Windows with ALSA/CoreMIDI/WinMM)

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
- `-p, --control-port <PORT>`: Starting control port - each MIDI port gets consecutive port pairs (default: 5004)
- `-b, --bind <ADDR>`: Bind address (default: "0.0.0.0")
- `--monitor-interval <SECONDS>`: Port monitoring interval in seconds (default: 5)
- `-l, --log-level <LEVEL>`: Log level (trace, debug, info, warn, error)

The server will:
1. Detect available MIDI input and output ports
2. Monitor for port changes in the background
3. **Advertise each MIDI port via separate mDNS entry** with service type `_apple-midi._udp`
4. Allocate consecutive UDP port pairs for each MIDI port (control + data)
5. Listen for incoming connections on all advertised ports
6. Handle AppleMIDI session handshakes and MIDI data transport

**Multi-Port Advertising**: Following Apple MIDI's approach, each physical MIDI port (input or output) is advertised as a separate mDNS service. This ensures maximum compatibility with Apple Audio MIDI Setup and other RTP-MIDI implementations. Each service includes TXT records with port metadata (name, type, index).

### Client

The client uses subcommands for better user experience.

Discover services on the network:

Options:
- `-H, --host <HOST>`: Remote host to connect to (required for connect)
- `-p, --port <PORT>`: Remote control port (default: 5004)
- `-n, --name <NAME>`: Device name (defaults to system hostname)
- `-b, --bind <ADDR>`: Local bind address (default: "0.0.0.0:0")
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
- **One mDNS entry per MIDI port** for Apple MIDI compatibility
- TXT records: `name=<port_name>`, `ver=2`, `type=<input|output>`, `index=<port_index>`
- Automatically discoverable by macOS Audio MIDI Setup

## Testing

Run the protocol tests:

```bash
cargo test
```

### Manual Testing

1. Start the server on one machine:
   ```bash
   ./target/release/nmidi-server --name "TestServer"
   ```

2. From another machine (or terminal), browse for services:
   ```bash
   ./target/release/nmidi-client discover
   ```

3. Connect to the server:
   ```bash
   ./target/release/nmidi-client connect --host <server-ip>
   ```
   ```

4. On macOS, open "Audio MIDI Setup" → "MIDI Studio" → "Network" to see the advertised service

## License

MIT

## References

- [RFC 6295 - RTP Payload Format for MIDI](https://www.rfc-editor.org/rfc/rfc6295.html)
- [Apple MIDI Network Driver Protocol](https://developer.apple.com/library/archive/documentation/Audio/Conceptual/MIDINetworkDriverProtocol/)
- [RFC 6762 - Multicast DNS](https://datatracker.ietf.org/doc/html/rfc6762)
- [RFC 6763 - DNS-Based Service Discovery](https://datatracker.ietf.org/doc/html/rfc6763)
