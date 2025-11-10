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


# Coordinator-Worker Flow Explanation

## Overview
Your system uses **asyncio Protocol** for the coordinator (server) and **asyncio StreamWriter/Reader** for workers (clients).

---

## STARTUP SEQUENCE

### Coordinator Startup (coordinator.py main())
```
1. initializeFiles() → Creates empty primes.txt
2. loadFiles(["dataset_001.txt", "dataset_002.txt", "dataset_003.txt"])
   → Creates 3 Task objects
   → Adds to CoordinatorProtocol.pending_tasks = [Task1, Task2, Task3]
   → task_counter = 3

3. Creates 3 asyncio tasks:
   a) newCoordinator() → Listens on localhost:5000
   b) flushBuffer() → Writes primes to disk every 0.5s
   c) periodicSnapshot() → Takes snapshot every 30s
```

### Worker Startup (worker.py main())
```
1. Creates WorkerClient(workerID, serverAddrs, k=5)
2. Calls worker.connect("localhost", 5000)
   → Opens TCP connection to coordinator
   → Starts 2 background tasks:
      a) handle_messages() → Listen for messages
      b) send_heartbeat() → Send heartbeat every 5s
3. Waits in event loop
```

---

## INITIAL CONNECTION & REGISTRATION

```
WORKER                                COORDINATOR
  |                                        |
  |---- TCP connect ------>               |
  |                                        |
  |                  <---- request_id ---- |
  |             (CoordinatorProtocol.connection_made())
  |
  |---- register msg ---->                |
  |   {'type': 'register',                |
  |    'worker_id': 'worker-1'}           |
  |                                        |
  |                  <---- registered ---- |
  |             (registerHandler() called)
  |                                        |
  |                    Now wState=IDLE     |
  |                    Calls assignTask()  |
  |                                        |
  |---- ready to receive ------>          |
  |      task_assignment msg              |
```

**What happens in registerHandler():**
```python
def registerHandler(self, message):
    self.wID = 'worker-1'                              # Store worker ID
    CoordinatorProtocol.workers['worker-1'] = self     # Add to workers dict
    self.wState = 'idle'                               # Mark as IDLE
    self.sendMsg({'type': 'registered', ...})          # Send acknowledgement
    self.assignTask()                                  # Try to assign a task
```

---

## TASK ASSIGNMENT FLOW

**After worker registers:**

```
COORDINATOR.assignTask()
│
├─ Check: Is worker IDLE? YES ✓
├─ Check: Are there pending tasks? YES ✓
│
├─ Pop task from pending_tasks
│  pending_tasks was: [Task1, Task2, Task3]
│  pending_tasks now: [Task2, Task3]
│  task = Task1
│
├─ Mark task as assigned:
│  task.assigned_to = 'worker-1'
│  task.assigned_at = time.time()
│  self.current_task = Task1
│  self.wState = 'busy'
│
└─ Send to worker:
   {
     'type': 'task_assignment',
     'task_id': 1,
     'filename': 'input_dataset_001.txt',
     'priority': 0
   }
```

**Worker receives task_assignment:**

```python
async def taskAssignmentHandler(self, message):
    task_id = 1
    filename = 'input_dataset_001.txt'
    
    # Create async task to process file
    asyncio.create_task(self.process_file(task_id, filename))
    # ⚠️ NOTE: This returns IMMEDIATELY (non-blocking)
```

---

## FILE PROCESSING (Worker)

```
process_file(task_id=1, filename='input_dataset_001.txt')
│
├─ 1. Open file via AFS
│      await self.client.open('input_dataset_001.txt')
│      → RPC call to AFS server
│      → File cached locally
│      → Returns file content as bytes
│
├─ 2. Parse numbers
│      content = bytesResp.decode('utf-8')
│      numbers = [2, 3, 5, 7, 11, ...]  (from file)
│
├─ 3. Test primality
│      primes = []
│      for num in numbers:
│          if self._is_prime(num):        # Miller-Rabin test
│              primes.append(num)
│
├─ 4. Send primes_result to coordinator
│      await self.send_message({
│          'type': 'primes_result',
│          'task_id': 1,
│          'filename': 'input_dataset_001.txt',
│          'primes': [2, 3, 5, 7, 11, 13, ...],
│          'worker_id': 'worker-1'
│      })
│
├─ 5. Increment counter
│      self.tasks_processed += 1
│
├─ 6. Send task_complete
│      await self.send_message({
│          'type': 'task_complete',
│          'task_id': 1,
│          'result': 'Found 8 primes'
│      })
│
└─ 7. Close file
       await self.client.close(filename)
```

---

## COORDINATOR RECEIVES RESULTS

### When primes_result arrives:

```python
def primesHandler(self, message):
    id = 1
    filename = 'input_dataset_001.txt'
    primes = [2, 3, 5, 7, 11, 13, ...]
    
    # Buffer for writing to disk (will be flushed periodically)
    data = {
        'task_id': 1,
        'filename': 'input_dataset_001.txt',
        'primes': [...],
        'worker_id': 'worker-1',
        'timestamp': time.time()
    }
    
    CoordinatorProtocol.buffer_writer.append(data)
    # ⚠️ NOT written to disk yet! Buffered.
    
    print(f"Received {len(primes)} primes from worker worker-1")
```

**Separately, flushBuffer() coroutine writes periodically:**

```python
async def flushBuffer(cls):
    while True:
        await asyncio.sleep(0.5)  # Every 0.5 seconds
        
        if cls.buffer_writer:
            async with cls.writer_lock:  # Lock for thread safety
                results_to_write = cls.buffer_writer[:]
                cls.buffer_writer.clear()
                
                with open('primes.txt', 'a') as f:
                    for result in results_to_write:
                        for prime in result['primes']:
                            if prime not in existingPrimes:  # Deduplicate
                                f.write(f"{prime}\n")
                                existingPrimes.add(prime)
```

### When task_complete arrives:

```python
def taskCompletionHandler(self, message):
    task_id = 1
    
    # Find the current task for this worker
    if self.current_task and self.current_task.task_id == task_id:
        self.current_task.completed = True
        CoordinatorProtocol.completed_tasks.append(self.current_task)
        self.current_task = None
        self.completed_tasks += 1  # Per-worker counter
    
    # Mark worker as IDLE again
    self.wState = 'idle'
    
    # Try to assign another task
    self.assignTask()
    
    # If there's Task2 in pending_tasks:
    # → Task2 gets assigned to worker-1
    # → worker-1 starts processing immediately
```

---

## WHAT HAPPENS IF WORKER CRASHES MID-TASK?

```
Worker-1 is processing Task1
│
└─ TCP connection drops
   (Worker process killed)
   
COORDINATOR side:
│
└─ connection_lost() is called automatically
   │
   ├─ Print: "Worker worker-1 disconnected"
   ├─ Set wState = 'disconnected'
   ├─ Check: Does worker have current_task?
   │  YES! It has Task1
   │
   ├─ Reassign Task1:
   │  task.assigned_to = None
   │  task.assigned_at = None
   │  CoordinatorProtocol.pending_tasks.insert(0, task)
   │  # Task1 goes BACK to front of queue!
   │
   ├─ self.current_task = None
   │
   └─ Remove from workers dict
      CoordinatorProtocol.workers.pop('worker-1')
```

**Now pending_tasks looks like:**
```
[Task1, Task2, Task3]  ← Task1 moved back to front
```

**When another worker is IDLE:**
```
Worker-2 or Worker-3 calls assignTask()
│
└─ Pops Task1 from pending_tasks
   └─ Starts processing Task1
```

---

## SNAPSHOT FLOW

**Every 30 seconds, periodicSnapshot() runs:**

```python
async def initiateSnapshot(cls):
    snapshot_id = f"snapshot_{counter}_{timestamp}"
    
    # 1. Save coordinator state
    coordinator_state = {
        'pending_tasks': [Task1, Task2],      # What's left to do
        'completed_tasks': [old_tasks...],    # What's done
        'task_counter': 3,
        'workers': ['worker-1', 'worker-2', 'worker-3']
    }
    
    cls.snapshot_states[snapshot_id] = {
        'coordinator': coordinator_state,
        'workers': {},  # Will be filled by workers
        'timestamp': time.time()
    }
    
    # 2. Broadcast snapshot marker to all workers
    msg = {'type': 'snapshot_marker', 'snapshot_id': snapshot_id}
    
    for worker in cls.workers.values():
        worker.sendMsg(msg)
    
    # 3. Wait for workers to respond (2 seconds)
    await asyncio.sleep(2)
    
    # 4. Save to disk
    cls.save(snapshot_id)
```

**Worker receives snapshot_marker:**

```python
async def spanshotMarkerHandler(self, message):
    snapshot_id = message['snapshot_id']
    
    # Capture current state
    state = {
        'worker_id': 'worker-1',
        'tasks_processed': 5,           # How many completed
        'tasks_failed': 0,
        'cache_files': ['dataset_001.txt', 'dataset_002.txt'],
        'timestamp': time.time()
    }
    
    # Send back to coordinator
    await self.send_message({
        'type': 'snapshot_state',
        'snapshot_id': snapshot_id,
        'worker_id': 'worker-1',
        'state': state
    })
```

**Coordinator collects snapshot_state:**

```python
def snapshotStateHandler(self, message):
    snapshot_id = message['snapshot_id']
    worker_id = message['worker_id']
    state = message['state']
    
    if snapshot_id in cls.snapshot_states:
        # Add to snapshot
        cls.snapshot_states[snapshot_id]['workers'][worker_id] = state
        print(f"Collected state from {worker_id} for {snapshot_id}")
```

**Final snapshot JSON:**

```json
{
  "coordinator": {
    "pending_tasks": [...],
    "completed_tasks": [...],
    "task_counter": 3,
    "workers": ["worker-1", "worker-2", "worker-3"]
  },
  "workers": {
    "worker-1": {
      "worker_id": "worker-1",
      "tasks_processed": 1,
      "tasks_failed": 0,
      "cache_files": ["dataset_001.txt"],
      "timestamp": 1762764065.6389608
    },
    "worker-2": {...},
    "worker-3": {...}
  },
  "timestamp": 1762764065.180928
}
```

---

## KEY POINTS

1. **Pending Tasks Queue**: All unassigned tasks sit here
2. **Current Task**: Each worker can hold ONE task at a time
3. **Reassignment**: If worker dies mid-task, task goes back to front of queue
4. **Buffering**: Primes are buffered before writing to disk (durability + performance)
5. **Snapshots**: Periodic snapshots capture coordinator + all worker states
6. **Non-blocking**: `asyncio.create_task()` returns immediately, processing happens in background

---

## STATE DIAGRAM

```
         ┌─────────────┐
         │   CONNECT   │
         └──────┬──────┘
                │ registers
                ▼
         ┌─────────────┐
    ┌───►│    IDLE     │◄──┐
    │    └──────┬──────┘   │ task complete
    │           │          │
    │ task      │ task
    │ assign    │ assign
    │           ▼
    │    ┌─────────────┐
    └────│    BUSY     │
         └──────┬──────┘
                │ crash/disconnect
                ▼
         ┌─────────────┐
         │    DEAD     │
         └─────────────┘
         (task reassigned)
```