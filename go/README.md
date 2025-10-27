# HyperLiquid L1 Eval - Go Client

Go-based gRPC client for testing the HyperLiquid L1 Gateway.

## Setup

The protobuf files have already been generated from `hyperliquid.proto`. If you need to regenerate them:

```bash
# Install protobuf compiler tools (if not already installed)
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Generate protobuf files
PATH=$PATH:~/go/bin protoc --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  -I=.. ../hyperliquid.proto

# Move generated files to the correct location
mkdir -p internal/api
mv hyperliquid*.pb.go internal/api/
```

## Build

```bash
cd go
go build -o hyperliquid-l1-eval
```

## Usage

### Stream Blocks

```bash
./hyperliquid-l1-eval -gateway <host:port> -stream -data-type blocks -since 0
```

### Stream Block Fills

```bash
./hyperliquid-l1-eval -gateway <host:port> -stream -data-type fills -since 0
```

### Stream Orderbook Snapshots

```bash
./hyperliquid-l1-eval -gateway <host:port> -stream -data-type snapshots -since 0
```

### Get Single Orderbook Snapshot

```bash
./hyperliquid-l1-eval -gateway <host:port> -data-type snapshots -since 0
```

## Options

- `-gateway`: Gateway host:port to dial (default: `127.0.0.1:50051`)
- `-stream`: Stream data instead of fetching once
- `-data-type`: Data type: `blocks | fills | snapshots`
- `-since`: Unix ms timestamp to start from (0=latest)
- `-sparse-output`: Enable sparse output for streams
- `-tls`: Use TLS for the connection
- `-gzip`: Enable gzip compression
- `-max-recv-mb`: Max gRPC receive size in MB (default: 128)
- `-file`: Output file (appends, one message per line)
- `-file-raw`: Write raw bytes even with formatted logging
- `-print-headers`: Print response headers and trailers
- `-H`, `-header`: Outgoing header 'key:value' or 'key=value' (repeatable)

## Examples

### With sparse output

```bash
./hyperliquid-l1-eval -gateway localhost:50051 -stream -data-type blocks -sparse-output
```

### With file output

```bash
./hyperliquid-l1-eval -gateway localhost:50051 -stream -data-type fills -file output.jsonl
```

### With TLS and gzip

```bash
./hyperliquid-l1-eval -gateway api.example.com:443 -tls -gzip -stream -data-type blocks
```

### With custom headers

```bash
./hyperliquid-l1-eval -gateway localhost:50051 -H "authorization:Bearer token123" -stream -data-type blocks
```
