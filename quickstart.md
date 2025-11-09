# Quick Start Guide

## 5-Minute Setup

### 1. Prepare Data
```bash
# Create test input files
mkdir -p data/input data/output
cp /path/to/your/input_dataset_*.txt data/input/
```

### 2. Start System
```bash
# Simple mode (single server)
./start_system.sh simple 3

# OR Replicated mode (3 servers, fault-tolerant)
./start_system.sh replicated 3
```

The script will automatically:
- Start file server(s)
- Start coordinator
- Start 3 workers
- Create necessary directories

### 3. Monitor Progress
```bash
# Watch coordinator logs
tail -f logs/coordinator.log

# Watch worker logs
tail -f logs/worker*.log

# Check output file
watch -n 1 "wc -l primes.txt"
```

### 4. Stop System
```bash
./stop_system.sh
```

## What Each Number Parameter Means

```bash
./start_system.sh <mode> <num_workers>
#                   │        │
#                   │        └─ Number of worker processes (default: 3)
#                   └─ simple or replicated
```

## Quick Tests

### Test 1: Basic Functionality
```bash
# Start with 1 worker
./start_system.sh simple 1

# Wait 10 seconds
sleep 10

# Check output
cat primes.txt
```

### Test 2: Fault Tolerance
```bash
# Start replicated system
./start_system.sh replicated 3

# In another terminal, kill primary server
pkill -f "server1"

# System should continue working!
```

### Test 3: Worker Failure Recovery
```bash
# Start system
./start_system.sh simple 3

# Kill a worker
pkill -f "worker-2"

# Check that other workers continue
# Restart failed worker manually:
python3 worker.py worker-2
```

## Common Issues

### "Port already in use"
```bash
# Kill existing processes
./stop_system.sh

# Wait and retry
sleep 2
./start_system.sh simple 3
```

### "No input files found"
```bash
# Create sample data
for i in {1..3}; do
    seq 1 1000 | shuf > data/input/input_dataset_00$i.txt
done
```

### "Python module not found"
```bash
# Ensure you're using Python 3
python3 --version

# Should be 3.8+
```

## Directory Structure After Running

```
.
├── data/
│   ├── input/          # Your input files
│   └── output/         # Created files
├── logs/               # All process logs
│   ├── server.log
│   ├── coordinator.log
│   └── worker*.log
├── snapshots/          # System snapshots
│   └── snapshot_*.json
├── primes.txt          # Output: unique primes found
└── /tmp/afs/           # Client cache directory
```

## Expected Output

After running, you should see:
1. `primes.txt` containing unique prime numbers (one per line)
2. Logs showing worker activity
3. Snapshots being created every 30 seconds

Example `primes.txt`:
```
2
3
5
7
11
13
...
```