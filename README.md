# HyperLiquid gRPC Testing Suite

A comprehensive testing suite for validating HyperLiquid's gRPC API endpoints, message handling, and streaming capabilities.

## Test Scripts

- `BlockTimingTestSimple.py` - Block streaming test with latency and throughput analysis
- `BlockTimingTestWithFills.py` - Combined blocks and fills streaming test with concurrent analysis
- `hyperliquid.proto` - Protocol buffer definitions for API contract validation

## Quick Start

1. Set up virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Generate protobuf stubs:
```bash
python -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. hyperliquid.proto
```

## Running Tests

### Block Timing Test (Simple)
Tests block streaming with latency and throughput analysis:
```bash
python BlockTimingTestSimple.py <duration_minutes> <endpoint> [api_key]
```

Examples:
```bash
# With SSL and API key (5 minute test)
python BlockTimingTestSimple.py 5 grpc.hyperliquid.xyz:443 your-api-key

# Without SSL (local endpoint)
python BlockTimingTestSimple.py 5 192.168.1.100:50051

# With SSL, no API key
python BlockTimingTestSimple.py 5 grpc.hyperliquid.xyz:443
```

Validates:
- Block streaming connection stability
- Latency measurement (block timestamp → local receipt)
- Throughput analysis (blocks/second over time)
- Dropped block detection
- Large message handling (150MB limit)

Outputs:
- CSV file with timing data
- Statistical analysis (min/max/mean/median latency & throughput)
- PNG graphs showing latency and throughput over time

### Block + Fills Timing Test (Combined)
Tests simultaneous block and fill streaming:
```bash
python BlockTimingTestWithFills.py <duration_minutes> <endpoint> [api_key]
```

Examples:
```bash
# With SSL and API key
python BlockTimingTestWithFills.py 5 grpc.hyperliquid.xyz:443 your-api-key

# Without SSL (local)
python BlockTimingTestWithFills.py 5 192.168.1.100:50051
```

Validates:
- Concurrent stream handling
- Block and fill latency comparison
- Fill data integrity (coin, side, direction, liquidations)
- Transaction ID (tid) gap detection
- Multi-threaded connection stability

Outputs:
- Two CSV files (blocks and fills)
- Comparative latency analysis
- Throughput metrics for both streams
- Combined graphs showing all metrics

## Test Configuration

All tests use standardized gRPC options:
```python
options = [('grpc.max_receive_message_length', 150 * 1024 * 1024)]  # 150MB
```

SSL is auto-detected:
- Enabled by default for public endpoints
- Disabled for localhost, 127.0.0.1, and 192.168.x.x addresses

## Requirements

- Python 3.7+
- `grpcio>=1.60.0` - gRPC runtime
- `grpcio-tools>=1.60.0` - Protobuf compilation
- `matplotlib>=3.8.0` - Graph generation

## Troubleshooting

**Import errors for protobuf files**: Run the protoc generation command to create `hyperliquid_pb2.py` and `hyperliquid_pb2_grpc.py`.

**Connection failures**: Verify the endpoint address and port. Use `:443` for SSL endpoints, `:50051` for local non-SSL.

**Large message errors**: Tests are configured for 150MB messages. Adjust the `grpc.max_receive_message_length` option if needed.

**Matplotlib display errors**: If graphs don't display, check your system's GUI backend. CSV and PNG files are always saved regardless.