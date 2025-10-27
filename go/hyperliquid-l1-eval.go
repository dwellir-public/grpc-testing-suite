// hyperliquid-l1-eval
package main

import (
    "context"
    "crypto/tls"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "io"
    "os"
    "os/signal"
    "path/filepath"
    "strings"
    "syscall"
    "time"

    pb "hyperliquid-l1-eval/internal/api"
    "github.com/rs/zerolog"
    "google.golang.org/grpc"
    "google.golang.org/grpc/connectivity"
    "google.golang.org/grpc/credentials"
    "google.golang.org/grpc/credentials/insecure"
    _ "google.golang.org/grpc/encoding/gzip" // register gzip compressor
    "google.golang.org/grpc/metadata"
)

const (
    defaultMaxRecvSizeMB  = 128
    fallbackMaxRecvSizeMB = 4
    bufLineSeparator      = "\n"
    permDirWritable       = 0o755
    permFileWritable      = 0o644
)

// Build information populated via -ldflags like
// "-X main.commit=... -X main.version=... -X main.buildTimeUTC=...".
var (
    buildTimeUTC string
    commit       string
    version      string
)

// CLI flags
var (
    gatewayAddr   string
    dataType      string
    stream        bool
    sinceTs       int64
    sparseOutput  bool
    useTLS        bool
    useGzip       bool
    maxRecvSizeMB int
    fileOutput    string
    fileRaw       bool
    printHeaders  bool
    headers       headerList
)

// headerList collects repeatable -H/--header flags
// Format: key:value or key=value; keys are lowercased for gRPC metadata
type headerList []string

func (h *headerList) String() string { return strings.Join(*h, "; ") }
func (h *headerList) Set(v string) error {
    if strings.TrimSpace(v) == "" {
        return nil
    }
    *h = append(*h, v)
    return nil
}

func cliFlagSetup() {
    flag.StringVar(&gatewayAddr, "gateway", "127.0.0.1:50051", "gateway host:port to dial")
    flag.BoolVar(&stream, "stream", false, "If set, stream data instead of fetching once")
    flag.StringVar(&dataType, "data-type", "", "data type: blocks | fills | snapshots")
    flag.Int64Var(&sinceTs, "since", 0, "unix ms timestamp to start from (0=latest)")
    flag.BoolVar(&sparseOutput, "sparse-output", false, "If set, output is sparse for streams")
    flag.BoolVar(&useTLS, "tls", false, "If set, use TLS for the connection")
    flag.BoolVar(&useGzip, "gzip", false, "If set, enable gzip compression\nNote: this "+
        "automatically sets headers \"grpc-encoding: gzip\" and \"grpc-accept-encoding: gzip\"")
    flag.IntVar(&maxRecvSizeMB, "max-recv-mb", defaultMaxRecvSizeMB, "Max gRPC receive size (MB)")
    flag.StringVar(&fileOutput, "file", "", "Output file (appends, one message per line)")
    flag.BoolVar(&fileRaw, "file-raw", false, "If set, write raw bytes even with formatted logging")
    flag.BoolVar(&printHeaders,
        "print-headers",
        false,
        "If set, print response headers and trailers")
    flag.Var(&headers, "H", "Outgoing header 'key:value' or 'key=value'; repeatable")
    flag.Var(&headers, "header", "Alias for -H")

    flag.Usage = func() {
        out := flag.CommandLine.Output()
        _, _ = fmt.Fprintf(out, "Usage of %s:\n", os.Args[0])
        flag.PrintDefaults()
        _, _ = fmt.Fprintln(out, "\nBuild Info:")
        _, _ = fmt.Fprintf(out, "  Version:    %s\n", version)
        _, _ = fmt.Fprintf(out, "  Commit:     %s\n", commit)
        _, _ = fmt.Fprintf(out, "  Build time: %s\n", buildTimeUTC)
    }
}

// normalizeDataType accepts aliases and returns canonical value.
func normalizeDataType(in string) string {
    s := strings.TrimSpace(strings.ToLower(in))
    switch s {
    case "snapshot", "snapshots", "orderbook", "orderbook-snapshot", "orderbook-snapshots":
        return "orderbook-snapshots"
    case "block", "blocks":
        return "blocks"
    case "fill", "fills":
        return "fills"
    default:
        return s
    }
}

func dial(ctx context.Context) (*grpc.ClientConn, error) {
    var opts []grpc.DialOption
    if useTLS {
        config := &tls.Config{InsecureSkipVerify: false}
        creds := credentials.NewTLS(config)
        opts = append(opts, grpc.WithTransportCredentials(creds))
    } else {
        opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
    }
    if maxRecvSizeMB <= 0 {
        maxRecvSizeMB = fallbackMaxRecvSizeMB
    }
    opts = append(opts,
        grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvSizeMB*1024*1024)),
    )
    if useGzip {
        opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")))
    }
    conn, err := grpc.NewClient(gatewayAddr, opts...)
    if err != nil {
        return nil, err
    }
    conn.Connect()
    ctxDial, cancel := context.WithTimeout(ctx, 10*time.Second)
    defer cancel()
    if err := waitForConnectionReady(ctxDial, conn); err != nil {
        _ = conn.Close()
        return nil, err
    }
    return conn, nil
}

func waitForConnectionReady(ctx context.Context, conn *grpc.ClientConn) error {
    for {
        state := conn.GetState()
        switch state {
        case connectivity.Ready:
            return nil
        case connectivity.Shutdown:
            return errors.New("gRPC connection shut down before becoming ready")
        default:
            // Continue waiting until the connection transitions.
        }
        if !conn.WaitForStateChange(ctx, state) {
            if ctx.Err() != nil {
                return ctx.Err()
            }
            return errors.New("context done while waiting for gRPC connection state change")
        }
    }
}

// getFileAppender returns a writer that appends to fileOutput, or nil if not set.
func getFileAppender() (io.WriteCloser, error) {
    if fileOutput == "" {
        return nil, nil
    }
    if err := os.MkdirAll(filepath.Dir(fileOutput), permDirWritable); err != nil {
        return nil, err
    }
    f, err := os.OpenFile(fileOutput, os.O_CREATE|os.O_WRONLY|os.O_APPEND, permFileWritable)
    if err != nil {
        return nil, err
    }
    return f, nil
}

// logAndWrite displays formatted/raw output and mirrors it to the optional file sink.
func logAndWrite(log zerolog.Logger, raw []byte, formatted string, w io.Writer) {
    var toDisplay string
    if formatted != "" {
        toDisplay = formatted
    } else {
        toDisplay = string(raw)
    }
    log.Info().Msg(toDisplay)
    if w != nil {
        if fileRaw && formatted != "" {
            _, _ = w.Write(raw)
        } else {
            _, _ = w.Write([]byte(toDisplay))
        }
        _, _ = w.Write([]byte(bufLineSeparator))
    }
}

func logMD(log zerolog.Logger, label string, md metadata.MD) {
    if len(md) == 0 {
        log.Info().Msgf("No %s to log", label)
        return
    }
    log.Info().Msgf("Incoming %s", label)
    for k, vals := range md {
        log.Info().Msgf("  %s: %v", k, vals)
    }
}

func logStartupMessage(log zerolog.Logger) {
    log.Info().Msg("hyperliquid-l1-eval starting")
    log.Info().Msg("Args")
    log.Info().Msgf("  gateway: %s", gatewayAddr)
    log.Info().Msgf("  data type: %s", dataType)
    if sinceTs == 0 {
        log.Info().Msgf("  since: now")
    } else {
        log.Info().Msgf("  since: %d", sinceTs)
    }
    if stream {
        log.Info().Msg("  call type: stream")
    } else {
        log.Info().Msg("  call type: unary")
    }
    log.Info().Msgf("  use gzip: %v", useGzip)
    log.Info().Msgf("  file output: %s", fileOutput)
    log.Info().Msgf("  sparse output: %v", sparseOutput)
}

func main() {
    cliFlagSetup()
    flag.Parse()

    dataType = normalizeDataType(dataType)

    // Logger setup
    log := zerolog.New(
        zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.DateTime},
    ).With().Timestamp().Logger()

    logStartupMessage(log)

    // Dial gateway
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    conn, err := dial(ctx)
    if err != nil {
        log.Fatal().Err(err).Msg("failed to dial gateway")
    }
    defer func() { _ = conn.Close() }()

    client := pb.NewHyperLiquidL1GatewayClient(conn)
    ts := &pb.Timestamp{Timestamp: sinceTs}

    // Build outgoing metadata if any headers were provided
    rpcCtx := ctx
    if len(headers) > 0 {
        var pairs []string
        for _, hv := range headers {
            k, v, ok := strings.Cut(hv, ":")
            if !ok {
                k, v, _ = strings.Cut(hv, "=")
            }
            k = strings.ToLower(strings.TrimSpace(k))
            v = strings.TrimSpace(v)
            if k == "" || v == "" {
                continue
            }
            pairs = append(pairs, k, v)
        }
        if len(pairs) > 0 {
            md := metadata.Pairs(pairs...)
            rpcCtx = metadata.NewOutgoingContext(ctx, md)
            if printHeaders {
                log.Info().Msgf("Outgoing headers")
                for k, v := range md {
                    log.Info().Msgf("  %s: %v", k, v)
                }
            }
        }
    }

    // Optional file appender
    fileAppender, err := getFileAppender()
    if err != nil {
        log.Fatal().Err(err).Msg("failed to prepare output file")
    }
    if fileAppender != nil {
        defer func() { _ = fileAppender.Close() }()
    }

    // Set up the receive function based on the operation mode
    var recv func(context.Context)
    if stream {
        switch dataType {
        case "blocks":
            recv = func(_ context.Context) {
                stm, err := client.StreamBlocks(rpcCtx, ts)
                if err != nil {
                    log.Error().Err(err).Msg("StreamBlocks failed")
                    return
                }

                if printHeaders {
                    h, err := stm.Header()
                    if err != nil {
                        log.Warn().Err(err).Msg("failed to get stream headers")
                    } else {
                        logMD(log, "headers", h)
                    }
                }

                log.Info().Msg("Response data")
                for {
                    msg, err := stm.Recv()
                    if err != nil {
                        if printHeaders {
                            logMD(log, "trailers", stm.Trailer())
                        }
                        if !errors.Is(err, context.Canceled) {
                            log.Error().Err(err).Msg("blocks recv error")
                        }
                        return
                    }

                    var formatted string
                    if sparseOutput {
                        var blockData map[string]any
                        if err := json.Unmarshal(msg.GetData(), &blockData); err == nil {
                            if abci, ok := blockData["abci_block"].(map[string]any); ok {
                                roundVal := abci["round"]
                                var roundStr string
                                switch v := roundVal.(type) {
                                case float64:
                                    roundStr = fmt.Sprintf("%d", int64(v))
                                default:
                                    roundStr = fmt.Sprintf("%v", v)
                                }
                                if t, ok := abci["time"].(string); ok {
                                    formatted = fmt.Sprintf("  round: %s, time: %s", roundStr, t)
                                }
                            }
                        }
                    }
                    logAndWrite(log, msg.GetData(), formatted, fileAppender)
                }
            }
        case "orderbook-snapshots":
            recv = func(_ context.Context) {
                stm, err := client.StreamOrderBookSnapshots(rpcCtx, ts)
                if err != nil {
                    log.Error().Err(err).Msg("StreamOrderBookSnapshots failed")
                    return
                }

                if printHeaders {
                    h, err := stm.Header()
                    if err != nil {
                        log.Warn().Err(err).Msg("failed to get stream headers")
                    } else {
                        logMD(log, "headers", h)
                    }
                }

                log.Info().Msg("Response data")
                total := 0
                for {
                    msg, err := stm.Recv()
                    if err != nil {
                        if printHeaders {
                            logMD(log, "trailers", stm.Trailer())
                        }
                        if !errors.Is(err, context.Canceled) {
                            log.Error().Err(err).Msg("orderbook recv error")
                        }
                        return
                    }
                    raw := msg.GetData()
                    var formatted string
                    if sparseOutput {
                        total += len(raw)
                        formatted = fmt.Sprintf("Orderbook snapshot bytes total: %d", total)
                    }
                    logAndWrite(log, raw, formatted, fileAppender)
                }
            }
        case "fills":
            recv = func(_ context.Context) {
                stm, err := client.StreamBlockFills(rpcCtx, ts)
                if err != nil {
                    log.Error().Err(err).Msg("StreamBlockFills failed")
                    return
                }

                if printHeaders {
                    h, err := stm.Header()
                    if err != nil {
                        log.Warn().Err(err).Msg("failed to get stream headers")
                    } else {
                        logMD(log, "headers", h)
                    }
                }

                log.Info().Msg("Response data")
                for {
                    msg, err := stm.Recv()
                    if err != nil {
                        if printHeaders {
                            logMD(log, "trailers", stm.Trailer())
                        }
                        if !errors.Is(err, context.Canceled) {
                            log.Error().Err(err).Msg("fill recv error")
                        }
                        return
                    }
                    raw := msg.GetData()
                    var formatted string
                    if sparseOutput {
                        var fillData []any
                        if err := json.Unmarshal(raw, &fillData); err == nil && len(fillData) >= 2 {
                            addr := fmt.Sprintf("%v", fillData[0])
                            if addrStr, ok := fillData[0].(string); ok {
                                addr = addrStr
                            }
                            if obj, ok := fillData[1].(map[string]any); ok {
                                var tmStr string
                                switch v := obj["time"].(type) {
                                case float64:
                                    tmStr = fmt.Sprintf("%d", int64(v))
                                default:
                                    tmStr = fmt.Sprintf("%v", v)
                                }
                                formatted = fmt.Sprintf("address: %s, time: %s", addr, tmStr)
                            }
                        }
                    }
                    logAndWrite(log, raw, formatted, fileAppender)
                }
            }
        default:
            log.Error().Msgf("unknown data type for stream mode: %s", dataType)
            return
        }
    } else {
        // Get single snapshot
        switch dataType {
        case "orderbook-snapshots":
            recv = func(_ context.Context) {
                // Show progress while the snapshot is being downloaded (may be large)
                doneCh := make(chan struct{})
                go func() {
                    ticker := time.NewTicker(1 * time.Second)
                    defer ticker.Stop()
                    elapsed := 0
                    for {
                        select {
                        case <-ticker.C:
                            elapsed++
                            log.Info().Msgf("Downloading snapshot… %ds elapsed", elapsed)
                        case <-doneCh:
                            return
                        }
                    }
                }()

                var hdr, tr metadata.MD
                snapshot, err := client.GetOrderBookSnapshot(
                    rpcCtx,
                    ts,
                    grpc.Header(&hdr),
                    grpc.Trailer(&tr))
                close(doneCh)

                if printHeaders {
                    logMD(log, "headers", hdr)
                    logMD(log, "trailers", tr)
                }

                if err != nil {
                    log.Error().Err(err).Msg("GetOrderBookSnapshot failed")
                    return
                }
                // Parse wrapper to extract block and timestamp
                type wrapper struct {
                    Block     string `json:"block"`
                    Timestamp int64  `json:"timestamp"`
                }
                var w wrapper
                if err := json.Unmarshal(snapshot.GetData(), &w); err != nil {
                    log.Error().Err(err).Msg("failed to parse snapshot wrapper")
                } else {
                    snapshotTime := time.UnixMilli(w.Timestamp).UTC()
                    formatted := fmt.Sprintf(
                        "Snapshot %s at %d (%s)",
                        w.Block,
                        w.Timestamp,
                        snapshotTime,
                    )
                    logAndWrite(log, snapshot.GetData(), formatted, fileAppender)
                }
            }
        default:
            log.Error().Msgf("unknown data type for non-stream mode: %s", dataType)
            return
        }
    }

    if stream {
        // Handle ctrl-c for streaming mode
        sigCh := make(chan os.Signal, 1)
        signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

        go recv(ctx)

        <-sigCh
        log.Info().Msg("signal received, shutting down")
        cancel()
    } else {
        // For non-streaming, just run recv and exit when done
        recv(ctx)
    }
}
