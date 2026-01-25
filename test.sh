#!/bin/bash
# Test script for nmidi client and server

set -e

echo "Building nmidi..."
cargo build --release

echo ""
echo "Starting server in background..."
./target/release/nmidi-server --name "TestServer" --bind "127.0.0.1" > /tmp/server.log 2>&1 &
SERVER_PID=$!

# Wait for server to start
sleep 2

echo "Server started (PID: $SERVER_PID)"
echo "Server logs:"
head -n 10 /tmp/server.log

echo ""
echo "Testing client connection..."
timeout 5 ./target/release/nmidi-client --host 127.0.0.1 --port 5004 --name "TestClient" || true

echo ""
echo "Stopping server..."
kill $SERVER_PID || true

echo ""
echo "Test complete!"
