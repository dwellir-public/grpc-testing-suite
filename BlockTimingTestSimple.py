import grpc
import json
import csv
import time
import sys
import os
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
            elif 'header' in abci and isinstance(abci['header'], dict):
                if 'time' in abci['header']:
                    timestamp = abci['header']['time']
                elif 'timestamp' in abci['header']:
                    timestamp = abci['header']['timestamp']

            if 'round' in abci:
                round_num = abci['round']
            if 'parent_round' in abci:
                parent_round = abci['parent_round']

        if not timestamp:
            if 'timestamp' in block:
                timestamp = block['timestamp']
            elif 'time' in block:
                timestamp = block['time']

        if not round_num and 'round' in block:
            round_num = block['round']
        if not parent_round and 'parent_round' in block:
            parent_round = block['parent_round']

    except Exception as e:
        pass

    return timestamp, round_num, parent_round

def collect_data(duration_minutes, endpoint, use_ssl=True, api_key=None):
    """Collect block timing data for specified duration"""
    duration_seconds = duration_minutes * 60

    print('🚀 Hyperliquid Block Timing Test')
    print('=' * 60)
    print(f'📡 Endpoint: {endpoint}')
    print(f'🔒 SSL: {use_ssl}')
    print(f'🔑 API Key: {"Yes" if api_key else "No"}')
    print(f'⏱️  Duration: {duration_minutes} minutes ({duration_seconds} seconds)\n')

    options = [('grpc.max_receive_message_length', 150 * 1024 * 1024)]
    metadata = [('x-api-key', api_key)] if api_key else []

    # Setup CSV
    csv_file = f'block_timing_{int(time.time())}.csv'
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['block', 'block_timestamp', 'local_timestamp', 'diff_ms', 'blocks_per_sec', 'round', 'parent_round'])

        print(f'📊 Writing to: {csv_file}\n')

        # Create channel
        if use_ssl:
            credentials = grpc.ssl_channel_credentials()
            channel = grpc.secure_channel(endpoint, credentials, options=options)
        else:
            channel = grpc.insecure_channel(endpoint, options=options)

        with channel:
            client = hyperliquid_pb2_grpc.HyperLiquidL1GatewayStub(channel)
            request = hyperliquid_pb2.Timestamp(timestamp=0)

            block_count = 0
            recent_blocks = deque(maxlen=500)
            last_stats_print = time.time()
            prev_round = None
            dropped_blocks = 0
            start_time = time.time()

            print('📥 Streaming blocks...\n')

            try:
                for response in client.StreamBlocks(request, metadata=metadata):
                    local_ts = time.time()
                    elapsed = local_ts - start_time

                    # Check if duration reached
                    if elapsed >= duration_seconds:
                        print(f'\n⏱️  Duration reached: {duration_minutes} minutes')
                        break

                    local_ts_ms = int(local_ts * 1000)
                    block_count += 1

                    # Parse block
                    try:
                        block = json.loads(response.data.decode('utf-8'))
                        block_ts, round_num, parent_round = extract_block_data(block)
                    except:
                        block_ts = None
                        round_num = None
                        parent_round = None

                    # Check for dropped blocks
                    if prev_round is not None and round_num is not None:
                        if parent_round != prev_round:
                            dropped_blocks += 1
                            print(f'⚠️  DROPPED BLOCK! Expected parent_round={prev_round}, got {parent_round}')

                    prev_round = round_num

                    # Calculate diff
                    if block_ts:
                        try:
                            # stripping nanoseconds so `strptime` is able to parse block time
                            block_time = datetime.strptime(block_ts[:23], '%Y-%m-%dT%H:%M:%S.%f')
                            block_ts_int = block_time.timestamp()
                            diff_ms = local_ts_ms - block_ts_int
                        except:
                            diff_ms = None
                    else:
                        diff_ms = None

                    # Track for RPS calculation
                    recent_blocks.append(local_ts)

                    # Calculate blocks per second (last 5 seconds)
                    cutoff = local_ts - 5.0
                    recent = [t for t in recent_blocks if t > cutoff]
                    blocks_per_sec = len(recent) / 5.0 if len(recent) > 1 else 0

                    # Write to CSV
                    writer.writerow([
                        block_count,
                        block_ts if block_ts else '',
                        local_ts_ms,
                        diff_ms if diff_ms else '',
                        f'{blocks_per_sec:.2f}',
                        round_num if round_num else '',
                        parent_round if parent_round else ''
                    ])
                    f.flush()

                    # Print stats every 5 seconds
                    if local_ts - last_stats_print >= 5.0:
                        remaining = duration_seconds - elapsed
                        print(f'Block {block_count:,} | Round: {round_num} | {blocks_per_sec:.2f} blocks/sec | '
                              f'Dropped: {dropped_blocks} | Remaining: {int(remaining)}s')
                        last_stats_print = local_ts

            except KeyboardInterrupt:
                print(f'\n⚠️  Test interrupted by user')

            print(f'\n✅ Captured {block_count} blocks')
            print(f'📁 Saved to: {csv_file}')
            print(f'⚠️  Dropped blocks: {dropped_blocks}')

    return csv_file

def parse_timestamp(ts_str):
    """Parse ISO timestamp to milliseconds (treat as UTC)"""
    try:
        ts_clean = ts_str.split('+')[0].split('Z')[0]
        dt = datetime.fromisoformat(ts_clean)
        dt_utc = dt.replace(tzinfo=timezone.utc)
        return int(dt_utc.timestamp() * 1000)
    except:
        return None

def analyze_data(csv_file):
    """Analyze the collected timing data"""
    blocks = []
    latencies = []
    blocks_per_sec = []
    dropped_count = 0

    print('\n' + '=' * 60)
    print('📊 ANALYZING DATA')
    print('=' * 60 + '\n')

    # Read CSV
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        prev_round = None

        for row in reader:
            block_num = int(row['block'])
            local_ts = int(row['local_timestamp'])

            block_ts = parse_timestamp(row['block_timestamp'])

            round_num = int(row['round']) if row.get('round') and row['round'] else None
            parent_round = int(row['parent_round']) if row.get('parent_round') and row['parent_round'] else None

            # Check for dropped blocks
            if prev_round is not None and round_num is not None and parent_round is not None:
                if parent_round != prev_round:
                    dropped_count += 1

            prev_round = round_num

            # Calculate latency
            if block_ts:
                latency = local_ts - block_ts
                latencies.append(latency)
            else:
                latency = None

            bps = float(row['blocks_per_sec']) if row['blocks_per_sec'] else 0

            blocks.append({
                'block': block_num,
                'local_ts': local_ts,
                'block_ts': block_ts,
                'latency': latency,
                'bps': bps,
                'round': round_num,
                'parent_round': parent_round
            })

            if bps > 0:
                blocks_per_sec.append(bps)

    valid_latencies = [l for l in latencies if l is not None]

    print('📊 SUMMARY')
    print('=' * 60)
    print(f'Total blocks: {len(blocks)}')
    print(f'Duration: {(blocks[-1]["local_ts"] - blocks[0]["local_ts"]) / 1000:.1f} seconds')
    print(f'⚠️  Dropped blocks: {dropped_count}')
    if blocks[0]['round'] and blocks[-1]['round']:
        print(f'Round range: {blocks[0]["round"]} → {blocks[-1]["round"]}')
    print()

    # Latency stats
    if valid_latencies:
        print('⏱️  LATENCY (Block timestamp → Local receipt)')
        print('-' * 60)
        print(f'  Min:    {min(valid_latencies)/1000:>8.3f} sec')
        print(f'  Max:    {max(valid_latencies)/1000:>8.3f} sec')
        print(f'  Mean:   {statistics.mean(valid_latencies)/1000:>8.3f} sec')
        print(f'  Median: {statistics.median(valid_latencies)/1000:>8.3f} sec')
        if len(valid_latencies) > 1:
            print(f'  StdDev: {statistics.stdev(valid_latencies)/1000:>8.3f} sec')
        print()

        # Latency over time (10 buckets)
        bucket_size = len(blocks) // 10
        print('⏱️  LATENCY OVER TIME')
        print('-' * 60)
        for i in range(10):
            start = i * bucket_size
            end = start + bucket_size if i < 9 else len(blocks)
            bucket = [b['latency'] for b in blocks[start:end] if b['latency'] is not None]
            if bucket:
                avg = statistics.mean(bucket) / 1000
                print(f'  Blocks {blocks[start]["block"]:>6}-{blocks[end-1]["block"]:<6}: {avg:>7.3f} sec avg')
        print()

    # Blocks per second stats
    if blocks_per_sec:
        print('📈 BLOCKS PER SECOND')
        print('-' * 60)
        print(f'  Min:    {min(blocks_per_sec):>8.2f}')
        print(f'  Max:    {max(blocks_per_sec):>8.2f}')
        print(f'  Mean:   {statistics.mean(blocks_per_sec):>8.2f}')
        print(f'  Median: {statistics.median(blocks_per_sec):>8.2f}')
        if len(blocks_per_sec) > 1:
            print(f'  StdDev: {statistics.stdev(blocks_per_sec):>8.2f}')
        print()

        # BPS over time (10 buckets)
        bucket_size = len(blocks) // 10
        print('📈 BLOCKS/SEC OVER TIME')
        print('-' * 60)
        for i in range(10):
            start = i * bucket_size
            end = start + bucket_size if i < 9 else len(blocks)
            bucket = [b['bps'] for b in blocks[start:end] if b['bps'] > 0]
            if bucket:
                avg = statistics.mean(bucket)
                print(f'  Blocks {blocks[start]["block"]:>6}-{blocks[end-1]["block"]:<6}: {avg:>7.2f} blocks/sec')

    # Generate graphs
    print('\n📊 Generating graphs...')

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

    # Graph 1: Latency over time
    latency_blocks = [b['block'] for b in blocks if b['latency'] is not None]
    latency_values = [b['latency'] / 1000 for b in blocks if b['latency'] is not None]

    ax1.plot(latency_blocks, latency_values, linewidth=0.8, alpha=0.7)
    ax1.set_xlabel('Block Number')
    ax1.set_ylabel('Latency (seconds)')
    ax1.set_title('Latency: Block Timestamp → Local Receipt Time')
    ax1.grid(True, alpha=0.3)

    # Graph 2: Blocks per second over time
    bps_blocks = [b['block'] for b in blocks if b['bps'] > 0]
    bps_values = [b['bps'] for b in blocks if b['bps'] > 0]

    ax2.plot(bps_blocks, bps_values, linewidth=0.8, alpha=0.7, color='green')
    ax2.set_xlabel('Block Number')
    ax2.set_ylabel('Blocks/Second')
    ax2.set_title('Throughput: Blocks Per Second Over Time')
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()

    # Save figure
    output_file = csv_file.replace('.csv', '_analysis.png')
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f'✅ Saved graphs to: {output_file}')

    plt.show()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: python BlockTimingTestSimple.py <duration_in_minutes> <endpoint> [api_key]')
        print()
        print('Examples:')
        print('  # With SSL and API key:')
        print('  python BlockTimingTestSimple.py 5 grpc.hyperliquid.xyz:443 your-api-key')
        print()
        print('  # Without SSL (local):')
        print('  python BlockTimingTestSimple.py 5 192.168.1.100:50051')
        print()
        print('  # With SSL, no API key:')
        print('  python BlockTimingTestSimple.py 5 grpc.hyperliquid.xyz:443')
        sys.exit(1)

    try:
        duration_minutes = float(sys.argv[1])
    except ValueError:
        print('❌ Error: Duration must be a number (in minutes)')
        sys.exit(1)

    endpoint = sys.argv[2]
    api_key = sys.argv[3] if len(sys.argv) > 3 else None

    # Determine if SSL should be used (based on port or explicit localhost/IP)
    use_ssl = True
    if ':' in endpoint:
        host, port = endpoint.rsplit(':', 1)
        # Disable SSL for localhost, 127.0.0.1, or 192.168.x.x
        if host in ['localhost', '127.0.0.1'] or host.startswith('192.168.'):
            use_ssl = False

    # Collect data
    csv_file = collect_data(duration_minutes, endpoint, use_ssl, api_key)

    # Analyze data
    analyze_data(csv_file)
