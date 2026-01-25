# Contributing to nmidi

## Development Setup

### Prerequisites

- Rust 1.70 or later
- MIDI hardware (optional - for full testing)
- Linux: `libasound2-dev` package

### Building

```bash
cargo build
```

For release builds:

```bash
cargo build --release
```

### Testing

Run unit tests:

```bash
cargo test
```

Run integration tests (requires two terminals):

Terminal 1:
```bash
./target/release/nmidi-server --name "TestServer"
```

Terminal 2:
```bash
./target/release/nmidi-client --host localhost --port 5004
```

Or use the test script:
```bash
./test.sh
```

### Code Structure

- `nmidi-protocol/`: Core protocol implementation
  - `applemidi.rs`: AppleMIDI session management protocol
  - `rtp.rs`: RTP-MIDI packet encoding/decoding
  - `error.rs`: Error types

- `nmidi-server/`: Server binary
  - `main.rs`: Entry point and CLI
  - `discovery.rs`: mDNS service advertisement
  - `midi.rs`: MIDI port detection
  - `network.rs`: UDP socket management
  - `session.rs`: Session state and handshake logic

- `nmidi-client/`: Client binary
  - `main.rs`: Entry point and CLI
  - `discovery.rs`: mDNS service browsing
  - `network.rs`: UDP socket management

### Logging

Set the log level with the `--log-level` flag:

```bash
./target/release/nmidi-server --log-level debug
```

Available levels: trace, debug, info, warn, error

### Adding Features

When adding new features:

1. Update protocol tests in `nmidi-protocol/src/`
2. Add integration tests if applicable
3. Update README.md with new functionality
4. Ensure all tests pass: `cargo test`
5. Check for warnings: `cargo clippy`
6. Format code: `cargo fmt`

### Protocol Implementation Notes

#### AppleMIDI Handshake

1. Client sends `IN` (Invitation)
2. Server responds with `OK` (Invitation Accepted)
3. Both exchange `CK` (Synchronization) packets
4. Data exchange begins via RTP-MIDI packets

#### RTP-MIDI Packet Format

- RTP header (12 bytes)
- MIDI command section (variable)
- Recovery journal (optional, not yet implemented)

### Testing on Raspberry Pi

Cross-compile for ARM:

```bash
# Install cross-compilation tools
rustup target add armv7-unknown-linux-gnueabihf

# Build for Raspberry Pi
cargo build --release --target armv7-unknown-linux-gnueabihf
```

Copy to Pi and run:

```bash
scp target/armv7-unknown-linux-gnueabihf/release/nmidi-server pi@raspberrypi.local:~/
ssh pi@raspberrypi.local
./nmidi-server --name "RaspberryPi MIDI"
```

### Compatibility Testing

Test with macOS Audio MIDI Setup:

1. Start nmidi-server on Linux/Pi
2. Open Audio MIDI Setup on macOS
3. Open MIDI Studio
4. Click "Network" button
5. The server should appear in the directory
6. Connect and test MIDI routing

## Release Process

1. Update version in `Cargo.toml` files
2. Update CHANGELOG.md
3. Create git tag: `git tag v0.1.0`
4. Push tag: `git push origin v0.1.0`
5. Build release binaries for multiple platforms
6. Create GitHub release with binaries

## Getting Help

- Open an issue for bugs or feature requests
- Check existing issues for similar problems
- Provide logs with `--log-level debug` when reporting issues
