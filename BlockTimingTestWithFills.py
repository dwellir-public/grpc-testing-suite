import grpc
import json
import csv
import time
import sys
import threading
import statistics
from collections import deque
from datetime import datetime, timezone
import hyperliquid_pb2
import hyperliquid_pb2_grpc
import matplotlib.pyplot as plt

def extract_block_data(block):
    """Extract timestamp, round, and parent_round from block"""
    timestamp = None
    round_num = None
    parent_round = None

    try:
        if 'abci_block' in block:
            abci = block['abci_block']
            if 'timestamp' in abci:
                timestamp = abci['timestamp']
            elif 'time' in abci:
                timestamp = abci['time']
            if 'round' in abci:
                round_num = abci['round']
            if 'parent_round' in abci:
                parent_round = abci['parent_round']
    except:
        pass

    return timestamp, round_num, parent_round

def stream_blocks(endpoint, use_ssl, api_key, duration_seconds, csv_file, stats):
    """Stream blocks to CSV"""
    options = [('grpc.max_receive_message_length', 150 * 1024 * 1024)]
    metadata = [('x-api-key', api_key)] if api_key else []

    try:
        if use_ssl:
            credentials = grpc.ssl_channel_credentials()
            channel = grpc.secure_channel(endpoint, credentials, options=options)
        else:
            channel = grpc.insecure_channel(endpoint, options=options)

        print('📦 Blocks stream: Connecting...')

        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['block', 'block_timestamp', 'local_timestamp', 'diff_ms', 'blocks_per_sec', 'round', 'parent_round'])

            with channel:
                client = hyperliquid_pb2_grpc.HyperLiquidL1GatewayStub(channel)
                request = hyperliquid_pb2.Timestamp(timestamp=0)

                block_count = 0
                recent_blocks = deque(maxlen=500)
                prev_round = None
                dropped = 0
                start_time = time.time()

                print('📦 Blocks stream: Connected, receiving data...')

                for response in client.StreamBlocks(request, metadata=metadata):
                    local_ts = time.time()
                    if local_ts - start_time >= duration_seconds:
                        break

                    local_ts_ms = int(local_ts * 1000)
                    block_count += 1

                    try:
                        block = json.loads(response.data.decode('utf-8'))
                        block_ts, round_num, parent_round = extract_block_data(block)
                    except:
                        block_ts = None
                        round_num = None
                        parent_round = None

                    if prev_round is not None and round_num is not None:
                        if parent_round != prev_round:
                            dropped += 1

                    prev_round = round_num

                    diff_ms = None
                    if block_ts:
                        try:
                            diff_ms = local_ts_ms - int(block_ts)
                        except:
                            pass

                    recent_blocks.append(local_ts)
                    cutoff = local_ts - 5.0
                    recent = [t for t in recent_blocks if t > cutoff]
                    bps = len(recent) / 5.0 if len(recent) > 1 else 0

                    writer.writerow([
                        block_count,
                        block_ts if block_ts else '',
                        local_ts_ms,
                        diff_ms if diff_ms else '',
                        f'{bps:.2f}',
                        round_num if round_num else '',
                        parent_round if parent_round else ''
                    ])
                    f.flush()

                print(f'\n📦 Blocks stream: Complete ({block_count} blocks)')
                stats['blocks'] = block_count
                stats['blocks_dropped'] = dropped

    except grpc.RpcError as e:
        print(f'\n❌ Blocks stream gRPC error: {e.code()} - {e.details()}')
        stats['blocks_error'] = str(e)
    except Exception as e:
        print(f'\n❌ Blocks stream error: {e}')
        stats['blocks_error'] = str(e)

def stream_fills(endpoint, use_ssl, api_key, duration_seconds, csv_file, stats):
    """Stream block fills to CSV"""
    options = [('grpc.max_receive_message_length', 150 * 1024 * 1024)]
    metadata = [('x-api-key', api_key)] if api_key else []

    try:
        if use_ssl:
            credentials = grpc.ssl_channel_credentials()
            channel = grpc.secure_channel(endpoint, credentials, options=options)
        else:
            channel = grpc.insecure_channel(endpoint, options=options)

        print('💰 Fills stream: Connecting...')

        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['fill', 'fill_time', 'local_timestamp', 'diff_ms', 'fills_per_sec', 'tid', 'coin', 'side', 'dir', 'has_liquidation'])

            with channel:
                client = hyperliquid_pb2_grpc.HyperLiquidL1GatewayStub(channel)
                request = hyperliquid_pb2.Timestamp(timestamp=0)

                fill_count = 0
                recent_fills = deque(maxlen=500)
                prev_tid = None
                tid_gaps = 0
                start_time = time.time()

                print('💰 Fills stream: Connected, receiving data...')

                for response in client.StreamBlockFills(request, metadata=metadata):
                    local_ts = time.time()
                    if local_ts - start_time >= duration_seconds:
                        break

                    local_ts_ms = int(local_ts * 1000)
                    fill_count += 1

                    try:
                        fill_data = json.loads(response.data.decode('utf-8'))

                        # Parse [address, fill_info]
                        if isinstance(fill_data, list) and len(fill_data) == 2:
                            user_address, fill_info = fill_data
                            fill_time = fill_info.get('time')
                            tid = fill_info.get('tid')
                            coin = fill_info.get('coin', '')
                            side = fill_info.get('side', '')
                            direction = fill_info.get('dir', '')
                            has_liq = 1 if 'liquidation' in fill_info else 0
                        else:
                            fill_time = None
                            tid = None
                            coin = ''
                            side = ''
                            direction = ''
                            has_liq = 0

                    except:
                        fill_time = None
                        tid = None
                        coin = ''
                        side = ''
                        direction = ''
                        has_liq = 0

                    # Check for tid gaps (not sequential, but track large gaps)
                    if prev_tid is not None and tid is not None:
                        if tid - prev_tid > 100:  # Large gap threshold
                            tid_gaps += 1

                    prev_tid = tid

                    diff_ms = None
                    if fill_time:
                        try:
                            diff_ms = local_ts_ms - int(fill_time)
                        except:
                            pass

                    recent_fills.append(local_ts)
                    cutoff = local_ts - 5.0
                    recent = [t for t in recent_fills if t > cutoff]
                    fps = len(recent) / 5.0 if len(recent) > 1 else 0

                    writer.writerow([
                        fill_count,
                        fill_time if fill_time else '',
                        local_ts_ms,
                        diff_ms if diff_ms else '',
                        f'{fps:.2f}',
                        tid if tid else '',
                        coin,
                        side,
                        direction,
                        has_liq
                    ])
                    f.flush()

                print(f'\n💰 Fills stream: Complete ({fill_count} fills)')
                stats['fills'] = fill_count
                stats['tid_gaps'] = tid_gaps

    except grpc.RpcError as e:
        print(f'\n❌ Fills stream gRPC error: {e.code()} - {e.details()}')
        stats['fills_error'] = str(e)
    except Exception as e:
        print(f'\n❌ Fills stream error: {e}')
        stats['fills_error'] = str(e)

def collect_data(duration_minutes, endpoint, use_ssl, api_key):
    """Collect both blocks and fills data"""
    duration_seconds = duration_minutes * 60

    print('🚀 Hyperliquid Blocks + Fills Timing Test')
    print('=' * 60)
    print(f'📡 Endpoint: {endpoint}')
    print(f'🔒 SSL: {use_ssl}')
    print(f'🔑 API Key: {"Yes" if api_key else "No"}')
    print(f'⏱️  Duration: {duration_minutes} minutes\n')

    blocks_csv = f'block_timing_{int(time.time())}.csv'
    fills_csv = f'fill_timing_{int(time.time())}.csv'

    print(f'📊 Blocks CSV: {blocks_csv}')
    print(f'📊 Fills CSV: {fills_csv}\n')

    # Shared stats
    stats = {
        'blocks': 0,
        'blocks_dropped': 0,
        'fills': 0,
        'tid_gaps': 0
    }

    # Start both threads
    blocks_thread = threading.Thread(
        target=stream_blocks,
        args=(endpoint, use_ssl, api_key, duration_seconds, blocks_csv, stats)
    )
    fills_thread = threading.Thread(
        target=stream_fills,
        args=(endpoint, use_ssl, api_key, duration_seconds, fills_csv, stats)
    )

    print('📥 Starting both streams...\n')
    start_time = time.time()

    blocks_thread.start()
    fills_thread.start()

    # Monitor progress
    try:
        while blocks_thread.is_alive() or fills_thread.is_alive():
            elapsed = time.time() - start_time
            remaining = duration_seconds - elapsed
            if remaining > 0:
                print(f'\r⏱️  {int(remaining)}s remaining | Blocks: {stats["blocks"]:,} | Fills: {stats["fills"]:,}',
                      end='', flush=True)
            time.sleep(1)
    except KeyboardInterrupt:
        print('\n⚠️  Interrupted by user')

    blocks_thread.join()
    fills_thread.join()

    print(f'\n\n✅ Collection complete')
    print(f'  Blocks: {stats["blocks"]:,} (dropped: {stats["blocks_dropped"]})')
    print(f'  Fills: {stats["fills"]:,} (tid gaps: {stats["tid_gaps"]})')

    return blocks_csv, fills_csv

def parse_timestamp(ts_str):
    """Parse ISO timestamp to milliseconds (UTC)"""
    try:
        ts_clean = ts_str.split('+')[0].split('Z')[0]
        dt = datetime.fromisoformat(ts_clean)
        dt_utc = dt.replace(tzinfo=timezone.utc)
        return int(dt_utc.timestamp() * 1000)
    except:
        return None

def analyze_data(blocks_csv, fills_csv):
    """Analyze both blocks and fills data"""
    print('\n' + '=' * 60)
    print('📊 ANALYSIS')
    print('=' * 60)

    # Analyze blocks
    print('\n📦 BLOCKS')
    print('-' * 60)

    blocks = []
    with open(blocks_csv, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['block_timestamp']:
                block_ts = parse_timestamp(row['block_timestamp'])
                local_ts = int(row['local_timestamp'])
                latency = (local_ts - block_ts) / 1000 if block_ts else None
                bps = float(row['blocks_per_sec']) if row['blocks_per_sec'] else 0
                blocks.append({'latency': latency, 'bps': bps})

    if blocks:
        valid_lat = [b['latency'] for b in blocks if b['latency'] is not None]
        valid_bps = [b['bps'] for b in blocks if b['bps'] > 0]

        if valid_lat:
            print(f'  Latency: {statistics.mean(valid_lat):.3f}s avg (min:{min(valid_lat):.3f} max:{max(valid_lat):.3f})')
        if valid_bps:
            print(f'  Throughput: {statistics.mean(valid_bps):.2f} blocks/sec avg')

    # Analyze fills
    print('\n💰 FILLS')
    print('-' * 60)

    fills = []
    liquidations = 0
    with open(fills_csv, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['fill_time']:
                fill_time = int(row['fill_time'])
                local_ts = int(row['local_timestamp'])
                latency = (local_ts - fill_time) / 1000
                fps = float(row['fills_per_sec']) if row['fills_per_sec'] else 0
                has_liq = int(row['has_liquidation']) if row['has_liquidation'] else 0
                liquidations += has_liq
                fills.append({'latency': latency, 'fps': fps})

    if fills:
        valid_lat = [f['latency'] for f in fills if f['latency'] is not None]
        valid_fps = [f['fps'] for f in fills if f['fps'] > 0]

        if valid_lat:
            print(f'  Latency: {statistics.mean(valid_lat):.3f}s avg (min:{min(valid_lat):.3f} max:{max(valid_lat):.3f})')
        if valid_fps:
            print(f'  Throughput: {statistics.mean(valid_fps):.2f} fills/sec avg')
        print(f'  Liquidations: {liquidations}')

    # Generate comparison graphs
    print('\n📊 Generating graphs...')

    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))

    # Blocks latency
    if blocks:
        block_nums = list(range(1, len(blocks) + 1))
        block_lats = [b['latency'] for b in blocks if b['latency'] is not None]
        ax1.plot(block_nums[:len(block_lats)], block_lats, linewidth=0.5, alpha=0.7)
        ax1.set_xlabel('Block Number')
        ax1.set_ylabel('Latency (seconds)')
        ax1.set_title('Blocks: Latency Over Time')
        ax1.grid(True, alpha=0.3)

    # Blocks throughput
    if blocks:
        block_bps = [b['bps'] for b in blocks if b['bps'] > 0]
        ax2.plot(range(1, len(block_bps) + 1), block_bps, linewidth=0.5, alpha=0.7, color='green')
        ax2.set_xlabel('Block Number')
        ax2.set_ylabel('Blocks/Second')
        ax2.set_title('Blocks: Throughput Over Time')
        ax2.grid(True, alpha=0.3)

    # Fills latency
    if fills:
        fill_nums = list(range(1, len(fills) + 1))
        fill_lats = [f['latency'] for f in fills if f['latency'] is not None]
        ax3.plot(fill_nums[:len(fill_lats)], fill_lats, linewidth=0.5, alpha=0.7, color='orange')
        ax3.set_xlabel('Fill Number')
        ax3.set_ylabel('Latency (seconds)')
        ax3.set_title('Fills: Latency Over Time')
        ax3.grid(True, alpha=0.3)

    # Fills throughput
    if fills:
        fill_fps = [f['fps'] for f in fills if f['fps'] > 0]
        ax4.plot(range(1, len(fill_fps) + 1), fill_fps, linewidth=0.5, alpha=0.7, color='purple')
        ax4.set_xlabel('Fill Number')
        ax4.set_ylabel('Fills/Second')
        ax4.set_title('Fills: Throughput Over Time')
        ax4.grid(True, alpha=0.3)

    plt.tight_layout()

    output_file = blocks_csv.replace('.csv', '_combined_analysis.png')
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f'✅ Saved graphs to: {output_file}')

    plt.show()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: python BlockTimingTestWithFills.py <duration_minutes> <endpoint> [api_key]')
        print()
        print('Examples:')
        print('  python BlockTimingTestWithFills.py 5 grpc.hyperliquid.dwellir:443 your-api-key')
        print('  python BlockTimingTestWithFills.py 5 192.168.1.100:50051')
        sys.exit(1)

    try:
        duration_minutes = float(sys.argv[1])
    except ValueError:
        print('❌ Error: Duration must be a number')
        sys.exit(1)

    endpoint = sys.argv[2]
    api_key = sys.argv[3] if len(sys.argv) > 3 else None

    # Auto-detect SSL and add default port
    use_ssl = True
    if ':' in endpoint:
        host = endpoint.rsplit(':', 1)[0]
        if host in ['localhost', '127.0.0.1'] or host.startswith('192.168.'):
            use_ssl = False
    else:
        # No port specified, add default
        if endpoint in ['localhost', '127.0.0.1'] or endpoint.startswith('192.168.'):
            use_ssl = False
            endpoint = f'{endpoint}:50051'
        else:
            use_ssl = True
            endpoint = f'{endpoint}:443'

    # Collect
    blocks_csv, fills_csv = collect_data(duration_minutes, endpoint, use_ssl, api_key)

    # Analyze
    analyze_data(blocks_csv, fills_csv)
