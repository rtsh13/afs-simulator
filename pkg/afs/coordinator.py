import socket
import asyncio
import json
import os
import time
import subprocess
import sys

WORKER_STATES = {'IDLE': 'idle','BUSY': 'busy','DISCONNECTED': 'disconnected'}

# this is responsible for all comms with worker connections
class CoordinatorProtocol(asyncio.Protocol):
    workers = {}
    pending_tasks = []
    completed_tasks = []
    task_counter = 0
    buffer_writer = []
    writer_lock = asyncio.Lock()
    output_file = "primes.txt"
    available_files = []
    assigned_files = {}
    snapshot_counter = 0
    snapshot_dir = "snapshots"
    
    snapshot_in_progress = False
    snapshot_markers_sent = set()
    snapshot_states = {}
    current_snapshot_id = None

    def __init__(self): 
        self.wID = None
        self.wState = WORKER_STATES['DISCONNECTED']
        self.transport = None
        self.current_task = None
        self.completed_tasks = 0
        self.connected_at = None
        self.last_heartbeat = None
        self.buffer = b""
        
    # triggered when worker connects
    # this function sig is required as part of asyncio library
    def connection_made(self, transport):
        self.transport = transport
        self.connected_at = time.time()
        self.sendMsg({'type': 'request_id', 'message': 'Provide worker ID'})

    # process responses from worker 
    # this function sig is required as part of asyncio library
    def data_received(self, data):
        self.buffer += data

        # we look for newline to stop processing a message
        while b'\n' in self.buffer:
            line, self.buffer = self.buffer.split(b'\n', 1)
            try:
                message = json.loads(line.decode())
                self.processMsg(message)
            except (json.JSONDecodeError, Exception) as e:
                print(f"Errror parsing message: {e}")

    # handles the conn being dropped off from the worker
    # this function sig is required as part of asyncio library
    def connection_lost(self, exc):
        if self.wID:
            print(f"Worker {self.wID} disconnected")
            self.wState = WORKER_STATES['DISCONNECTED']
            
            # Reassign current task if any
            if self.current_task:
                self.current_task.assigned_to = None
                self.current_task.assigned_at = None
                CoordinatorProtocol.pending_tasks.insert(0, self.current_task)
                self.current_task = None
            
            CoordinatorProtocol.workers.pop(self.wID, None)

    
    # handles all kinds of incoming and outgoing message routing for a coordinator
    # think of it as router.Mux in Golang context
    def processMsg(self, message):
        msg_type = message.get('type')
        
        if msg_type == 'register':
            self.handle_register(message)
        elif msg_type == 'heartbeat':
            self.handle_heartbeat(message)
        elif msg_type == 'task_complete':
            self.handle_task_complete(message)
        elif msg_type == 'task_failed':
            self.handle_task_failed(message)
        elif msg_type == 'primes_result':
            self.handle_primes_result(message)
        elif msg_type == 'status_request':
            self.handle_status_request()
        elif msg_type == 'snapshot_marker':
            self.handle_snapshot_marker(message)
        elif msg_type == 'snapshot_state':
            self.handle_snapshot_state(message)

    def handle_register(self, message):
        """Register a new worker"""
        self.worker_id = message.get('worker_id')
        CoordinatorProtocol.workers[self.worker_id] = self
        self.worker_state = WORKER_STATES['IDLE']
        
        print(f"Worker {self.worker_id} registered")
        self.send_message({'type': 'registered', 'worker_id': self.worker_id})
        self.try_assign_task()

    def handle_heartbeat(self, message):
        """Acknowledge worker heartbeat"""
        self.last_heartbeat = time.time()
        self.send_message({'type': 'heartbeat_ack', 'timestamp': time.time()})

    def handle_task_complete(self, message):
        """Handle task completion"""
        task_id = message.get('task_id')
        if self.current_task and self.current_task.task_id == task_id:
            self.current_task.completed = True
            CoordinatorProtocol.completed_tasks.append(self.current_task)
            self.current_task = None
            self.tasks_completed += 1
            
        self.worker_state = WORKER_STATES['IDLE']
        self.try_assign_task()

    def handle_task_failed(self, message):
        """Handle task failure - reassign task"""
        task_id = message.get('task_id')
        error = message.get('error', 'Unknown error')
        
        print(f"Task {task_id} failed on worker {self.worker_id}: {error}")
        
        if self.current_task and self.current_task.task_id == task_id:
            self.current_task.assigned_to = None
            self.current_task.assigned_at = None
            CoordinatorProtocol.pending_tasks.insert(0, self.current_task)
            self.current_task = None
            
        self.worker_state = WORKER_STATES['IDLE']
        self.try_assign_task()

    def handle_primes_result(self, message):
        """Receive and buffer prime results"""
        task_id = message.get('task_id')
        filename = message.get('filename')
        primes = message.get('primes', [])
        worker_id = message.get('worker_id')

        result_data = {
            'task_id': task_id,
            'filename': filename,
            'primes': primes,
            'worker_id': worker_id,
            'timestamp': time.time()
        }

        CoordinatorProtocol.write_buffer.append(result_data)
        print(f"Received {len(primes)} primes from worker {worker_id} for file {filename}")

    def handle_status_request(self):
        """Send current status to worker"""
        status = {
            'type': 'status',
            'workers': len(CoordinatorProtocol.workers),
            'pending_tasks': len(CoordinatorProtocol.pending_tasks),
            'completed_tasks': len(CoordinatorProtocol.completed_tasks),
            'buffer_size': len(CoordinatorProtocol.write_buffer),
            'your_state': self.worker_state,
            'your_tasks_completed': self.tasks_completed
        }
        self.send_message(status)

    def handle_snapshot_marker(self, message):
        """Handle snapshot marker from worker (Chandy-Lamport)"""
        snapshot_id = message.get('snapshot_id')
        worker_id = message.get('worker_id')
        
        if snapshot_id not in CoordinatorProtocol.snapshot_states:
            CoordinatorProtocol.snapshot_states[snapshot_id] = {
                'workers': {},
                'channels': {},
                'timestamp': time.time()
            }
        
        print(f"Received snapshot marker {snapshot_id} from worker {worker_id}")

    def handle_snapshot_state(self, message):
        """Collect worker state for snapshot"""
        snapshot_id = message.get('snapshot_id')
        worker_id = message.get('worker_id')
        state = message.get('state')
        
        if snapshot_id in CoordinatorProtocol.snapshot_states:
            CoordinatorProtocol.snapshot_states[snapshot_id]['workers'][worker_id] = state
            print(f"Collected state from worker {worker_id} for snapshot {snapshot_id}")

    def try_assign_task(self):
        """Assign a task to this worker if idle"""
        if self.worker_state != WORKER_STATES['IDLE']:
            return
        if not CoordinatorProtocol.pending_tasks:
            return

        task = CoordinatorProtocol.pending_tasks.pop(0)
        task.assigned_to = self.worker_id
        task.assigned_at = time.time()
        self.current_task = task
        self.worker_state = WORKER_STATES['BUSY']

        self.send_message({
            'type': 'task_assignment',
            'task_id': task.task_id,
            'filename': task.filename,
            'priority': task.priority
        })

    def send_message(self, message):
        """Send message to worker"""
        try:
            data = json.dumps(message) + '\n'
            self.transport.write(data.encode())
        except Exception as e:
            print(f"Error sending message: {e}")

    @classmethod
    def initialize_files(cls, output_file):
        cls.output_file = output_file
        
        with open(output_file, 'w') as f:
            pass

    @classmethod
    def load_files_from_list(cls, filenames):
        for filename in filenames:
            cls.task_counter += 1
            task = Task(cls.task_counter, filename, priority=0)
            cls.pending_tasks.append(task)
        
        return len(filenames)

    @classmethod
    def add_task(cls, filename, priority=0):
        """Add a new task with priority"""
        cls.task_counter += 1
        task = Task(cls.task_counter, filename, priority)

        # Insert based on priority
        inserted = False
        for i, existing_task in enumerate(cls.pending_tasks):
            if task.priority > existing_task.priority:
                cls.pending_tasks.insert(i, task)
                inserted = True
                break
        
        if not inserted:
            cls.pending_tasks.append(task)

        cls.assign_task_with_load_balancing()
        return task.task_id

    @classmethod
    def assign_task_with_load_balancing(cls):
        """Assign tasks to least loaded worker"""
        if not cls.pending_tasks:
            return
        
        idle_workers = [w for w in cls.workers.values() 
                       if w.worker_state == WORKER_STATES['IDLE']]
        if not idle_workers:
            return

        # Find worker with fewest completed tasks
        best_worker = min(idle_workers, key=lambda w: w.tasks_completed)
        best_worker.try_assign_task()

    @classmethod
    async def write_results_to_file(cls):
        """Background task to write buffered results to file"""
        seen_primes = set()
        
        while True:
            await asyncio.sleep(0.5)

            if cls.write_buffer:
                async with cls.write_lock:
                    if cls.write_buffer:
                        results_to_write = cls.write_buffer[:]
                        cls.write_buffer.clear()

                        with open(cls.output_file, 'a') as f:
                            for result in results_to_write:
                                # Write unique primes only
                                for prime in result['primes']:
                                    if prime not in seen_primes:
                                        f.write(f"{prime}\n")
                                        seen_primes.add(prime)

    @classmethod
    def get_system_status(cls):
        """Get comprehensive system status"""
        worker_stats = {}
        for worker_id, worker in cls.workers.items():
            if worker.connected_at:
                uptime = time.time() - worker.connected_at
                worker_stats[worker_id] = {
                    'state': worker.worker_state,
                    'current_task': worker.current_task.task_id if worker.current_task else None,
                    'tasks_completed': worker.tasks_completed,
                    'uptime_seconds': uptime
                }
        
        status = {
            'timestamp': time.time(),
            'total_workers': len(cls.workers),
            'idle_workers': sum(1 for w in cls.workers.values() 
                               if w.worker_state == WORKER_STATES['IDLE']),
            'busy_workers': sum(1 for w in cls.workers.values() 
                               if w.worker_state == WORKER_STATES['BUSY']),
            'pending_tasks': len(cls.pending_tasks),
            'completed_tasks': len(cls.completed_tasks),
            'buffer_size': len(cls.buffer_writer),
            'workers': worker_stats
        }
        return status

    @classmethod
    async def initiate_snapshot(cls):
        """Initiate Chandy-Lamport snapshot"""
        cls.snapshot_counter += 1
        snapshot_id = f"snapshot_{cls.snapshot_counter}_{int(time.time())}"
        cls.current_snapshot_id = snapshot_id
        cls.snapshot_in_progress = True
        
        print(f"Initiating snapshot {snapshot_id}")
        
        # Save coordinator state
        coordinator_state = {
            'pending_tasks': [{'task_id': t.task_id, 'filename': t.filename} 
                            for t in cls.pending_tasks],
            'completed_tasks': [{'task_id': t.task_id, 'filename': t.filename} 
                              for t in cls.completed_tasks[-100:]],
            'task_counter': cls.task_counter,
            'workers': list(cls.workers.keys())
        }
        
        cls.snapshot_states[snapshot_id] = {
            'coordinator': coordinator_state,
            'workers': {},
            'timestamp': time.time()
        }
        
        # Send marker to all workers
        marker_message = {
            'type': 'snapshot_marker',
            'snapshot_id': snapshot_id
        }
        
        for worker in cls.workers.values():
            worker.send_message(marker_message)
        
        # Wait for worker states
        await asyncio.sleep(2)
        
        # Save snapshot to file
        cls.save_snapshot(snapshot_id)
        cls.snapshot_in_progress = False

    # Save snapshot to file
    @classmethod
    def save(cls, snapshot_id):
        if not os.path.exists(cls.snapshot_dir):
            os.makedirs(cls.snapshot_dir)
        
        snapshot_file = os.path.join(cls.snapshot_dir, f"{snapshot_id}.json")
        with open(snapshot_file, 'w') as f: json.dump(cls.snapshot_states[snapshot_id], f, indent=2)
        
        print(f"snapshot {snapshot_id} saved to {snapshot_file}")

    @classmethod
    def restore_from_snapshot(cls, snapshot_id):
        snapshot_file = os.path.join(cls.snapshot_dir, f"{snapshot_id}.json")
        
        if not os.path.exists(snapshot_file):
            print(f"Snapshot {snapshot_id} not found")
            return False
        
        with open(snapshot_file, 'r') as f:
            snapshot_data = json.load(f)
        
        coordinator_state = snapshot_data['coordinator']
        
        # Restore pending tasks
        cls.pending_tasks = []
        for task_data in coordinator_state['pending_tasks']:
            task = Task(task_data['task_id'], task_data['filename'])
            cls.pending_tasks.append(task)
        
        cls.task_counter = coordinator_state['task_counter']
        
        print(f"Restored from snapshot {snapshot_id}")
        print(f"Restored {len(cls.pending_tasks)} pending tasks")
        
        return True

# think of this as a unit of work that worker will invoke
class Task(object):
    def __init__(self, task_id, filename, priority=0):
        self.task_id = task_id
        self.filename = filename
        self.priority = priority
        self.assignedTo = None
        self.assignedAt = None
        self.completed = False
        self.createdAt = time.time()

# start the new coordinator function
# indefinitely serve requests at port 5000
# port 5000 is arbitiary. Can include in args if needed
async def newCoordinator(host='localhost', port=5000):
    loop = asyncio.get_running_loop()
    server = await loop.create_server(lambda: CoordinatorProtocol(),host, port)
    
    print(f"Coordinator started on {host}:{port}")
    
    async with server:
        await server.serve_forever()

# take snapshots every 30 seconds.
# 30 is arbitiary number, we can tune/arg pass it if needed
async def periodicSnapshot(interval=30):
    while True:
        await asyncio.sleep(interval)
        await CoordinatorProtocol.initiateSnapshot()


async def main():
    output_file = "primes.txt"
    
    # helps clean prime txt file
    CoordinatorProtocol.initializeFiles(output_file)

    # we are assuming only 3 files exist that we need to work on. 
    # technically, this should be pulled from AFS workspace but for simplicity we hardcode it
    test_files = ["input_dataset_001.txt", "input_dataset_002.txt", "input_dataset_003.txt"]
    num_files = CoordinatorProtocol.loadFiles(test_files)
    print(f"Loaded {num_files} files for processing")
    
    # this kicks of a new bunch of tasks in the main event loop.
    coordinatorTask = asyncio.create_task(newCoordinator())
    bufferFlusher = asyncio.create_task(CoordinatorProtocol.flushBuffer())
    snapshotTask = asyncio.create_task(periodicSnapshot(interval=30))
    
    await asyncio.gather(coordinatorTask, bufferFlusher, snapshotTask)


if __name__ == "__main__":
    asyncio.run(main())