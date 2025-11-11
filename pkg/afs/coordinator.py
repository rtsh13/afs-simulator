import asyncio
import json
import os
import sys
import afsclient
import time
import coordinator_worker_task

WORKER_STATES = {'IDLE': 'idle','BUSY': 'busy','DISCONNECTED': 'disconnected'}

# this is responsible for all comms with worker connections
class CoordinatorProtocol(asyncio.Protocol):
    workers = {}
    pending_tasks = []
    completed_tasks = []
    task_counter = 0
    buffer_writer = []
    writer_lock = asyncio.Lock()
    # TODO Swap for Enviornment Variable
    output_file = "primes.txt"
    snapshot_counter = 0
    # TODO Swap for Environment variable
    snapshot_dir = "snapshots"
    
    snapshot_state = {}
    snapshot_in_progress = False

    afsclient = None

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
                CoordinatorProtocol.pending_tasks.insert(0, self.current_task)
                self.current_task = None
            
            CoordinatorProtocol.workers.pop(self.wID, None)

    
    # handles all kinds of incoming and outgoing message routing for a coordinator
    # think of it as router.Mux in Golang context
    def processMsg(self, message):
        msg_type = message.get('type')

        match msg_type:
            case 'register':
                self.registerHandler(message)
            case 'heartbeat':
                self.heartbeatHandler(message)
            case 'task_complete':
                self.taskCompletionHandler(message)
            case 'task_failed':
                self.taskFailedHandler(message)
            case 'primes_result':
                self.primesHandler(message)
            case 'status_request':
                self.statusHandler()

    # spawn a new worker and keep it IDLE
    def registerHandler(self, message):
        self.wID = message.get('worker_id')
        if self.wID in CoordinatorProtocol.workers:
            CoordinatorProtocol.workers[self.wID].terminate()
            CoordinatorProtocol.workers[self.wID] = None
        CoordinatorProtocol.workers[self.wID] = self
        self.wState = WORKER_STATES['IDLE']
        
        self.sendMsg({'type': 'registered', 'worker_id': self.wID})
        self.assignTask()

    # acknowledge the heartbeat of the worker
    def heartbeatHandler(self, message):
        now = time.time()
        self.last_heartbeat = now
        self.sendMsg({'type': 'heartbeat_ack', 'timestamp': now})

    # handle tasks being completed by the worker
    # throw them into IDLE state and see a new task
    def taskCompletionHandler(self, message):
        task_id = message.get('task_id')
        if self.current_task and self.current_task.task_id == task_id:
            self.current_task.completed = True
            CoordinatorProtocol.completed_tasks.append(self.current_task)
            index = -1
            for i in range(len(CoordinatorProtocol.pending_tasks)):
                if self.current_task.taskId == CoordinatorProtocol.pending_tasks.taskId:
                    index = i
                    break
            if index != -1:
                CoordinatorProtocol.pending_tasks.pop(index)
            self.current_task = None
            self.completed_tasks += 1
            
        self.wState = WORKER_STATES['IDLE']
        self.assignTask()

    # handle tasks being failed by the worker
    # assign new task
    # pie in the sky: add analytics to this to identify if a worker is crashing way too often
        # avoid assigning tasks based on predictions
    def taskFailedHandler(self, message):
        id = message.get('task_id')
        error = message.get('error', 'Unknown error')
        
        print(f"Task {id} failed on worker {self.wID}: {error}")
        
        if self.current_task and self.current_task.task_id == id:
            self.current_task = None
            
        self.wState = WORKER_STATES['IDLE']
        self.assignTask()

    # buffer the prime results received
    # we have an coroutine that will take care of flushing
    # fyi - this is how postgres works btw as well
    def primesHandler(self, message):
        primes = message.get('primes', [])

        CoordinatorProtocol.buffer_writer.extend(primes)

    def statusHandler(self):
        status = {
            'type': 'status',
            'workers': len(CoordinatorProtocol.workers),
            'pending_tasks': len(CoordinatorProtocol.pending_tasks),
            'completed_tasks': len(CoordinatorProtocol.completed_tasks),
            'buffer_size': len(CoordinatorProtocol.buffer_writer),
            'your_state': self.wState,
            'your_tasks_completed': self.completed_tasks
        }
        self.sendMsg(status)

    # only assign task if the worker is IDLE
    # pop from queue and send message to worker to start working
    # update state to BUSY
    # pie in sky: add concurrency to the worker to process more than one task?
    def assignTask(self):
        if self.wState != WORKER_STATES['IDLE']:
            return
        if not CoordinatorProtocol.pending_tasks:
            return

        task = CoordinatorProtocol.pending_tasks.pop(0)
        self.current_task = task
        self.wState = WORKER_STATES['BUSY']
        CoordinatorProtocol.pending_tasks.append(task)

        self.sendMsg({
            'type': 'task_assignment',
            'task_id': task.task_id,
            'filename': task.filename,
            'priority': task.priority
        })

    # coordinator sends arbitiary message to worker
    def sendMsg(self, message):
        try:
            data = json.dumps(message) + '\n'
            self.transport.write(data.encode())
        except Exception as e:
            print(f"Error sending message: {e}")

    # we are parsing the filenames and spinning one Task for each file
    # the tasks are duly noted in the pending tasks watcher
    @classmethod
    def loadFiles(cls, filenames):
        for filename in filenames:
            cls.task_counter += 1
            cls.pending_tasks.append(coordinator_worker_task.Task(cls.task_counter, filename, priority=0))
        
        return len(filenames)

    # adds task in the task pending queue
    @classmethod
    def addTask(cls, filename, priority=0):
        cls.task_counter += 1
        task = coordinator_worker_task.Task(cls.task_counter, filename, priority)

        # Insert based on priority
        inserted = False
        for i, existing_task in enumerate(cls.pending_tasks):
            if task.priority > existing_task.priority:
                cls.pending_tasks.insert(i, task)
                inserted = True
                break
        
        if not inserted:
            cls.pending_tasks.append(task)

        cls.loadbalance()
        return task.task_id

    # load balances task assignment
    # assigns the tasks to the work with the least amount of 
    @classmethod
    def loadbalance(cls):
        # Assign tasks to least loaded worker
        if not cls.pending_tasks:
            return
        
        idle_workers = [w for w in cls.workers.values() 
                       if w.worker_state == WORKER_STATES['IDLE']]
        if not idle_workers:
            return

        # Find worker with fewest completed tasks
        best_worker = min(idle_workers, key=lambda w: w.tasks_completed)
        best_worker.assignTask()

    # write items from the buffer to the file to maintain persistence
    @classmethod
    async def flushBuffer(cls):
        # init existing primes with primes from output file
        # if they already exist
        while True:
            # btw this is not same as time.sleep(0.5).
            # this tells the event loop to take .5 second shift in focus and peform other ops
            await asyncio.sleep(0.5)

            if cls.buffer_writer:
                existingPrimes = set()
                # we synchronise with others since buffer is shared resource
                async with cls.writer_lock:
                    if cls.buffer_writer:
                        # grab primes to write from buffer
                        results_to_write = cls.buffer_writer[:]
                        # Read in existing primes from file
                        bytesInFile = await cls.afsclient.open(cls.output_file)
                        content = bytesInFile.decode('utf-8')
                        content.strip()

                        for line in content.split('\n'):
                            line = line.strip()
                            if line:
                                try:
                                    num = int(line)
                                    results_to_write.append(num)
                                except ValueError:
                                    pass

                        # filter duplicate primes
                        filtered_results = []
                        for prime in results_to_write:
                            if prime not in existingPrimes:
                                filtered_results.append(prime)
                                existingPrimes.add(prime)

                        # convert to bytes for a newline separated set of string numbers
                        out_content = bytes("\n".join(str(prime) for prime in filtered_results), encoding='utf-8')
                        
                        # write bytes to file and flush output to afs server
                        cls.afsclient.write(cls.output_file, out_content)
                        cls.afsclient.close(cls.output_file)
                        
                        # clear buffer writer
                        cls.buffer_writer.clear()

    @classmethod
    def get_system_status(cls):

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

    # this is your chandy-lamport snapshot algo
    @classmethod
    async def initiateSnapshot(cls):
        cls.snapshot_counter += 1
        
        #snapshot_counter_time is name of the snapshot time

        cls.snapshot_in_progress = True
        
        # save the local coordinator state in the dict
        coordinator_state = {
            'pending_tasks': [{'task_id': t.task_id, 'filename': t.filename} for t in cls.pending_tasks],
            'completed_tasks': [{'task_id': t.task_id, 'filename': t.filename} for t in cls.completed_tasks[-50:]],
            'task_counter': cls.task_counter,
            'buffer_writer': [prime for prime in cls.buffer_writer]
        }
        
        cls.snapshot_state = {'coordinator': coordinator_state,'timestamp': time.time()}
        
        
        cls.save()
        cls.snapshot_in_progress = False

    # Save snapshot to file
    @classmethod
    def save(cls):
        if not os.path.exists(cls.snapshot_dir):
            os.makedirs(cls.snapshot_dir)
        
        if os.path.exists(os.path.join(cls.snapshot_dir, "snapshot_current.json")):
            if os.path.exists(os.path.join(cls.snapshot_dir, "snapshot_previous.json")):
                os.remove(os.paht.join(cls.snapshot_dir, "snapshot_previous.json"))
            os.rename(os.path.join(cls.snapshot_dir, "snapshot_current.json"), os.path.join(cls.snapshot_dir, "snapshot_previous.json"))

        snapshot_file = os.path.join(cls.snapshot_dir, "snapshot_current.json")
        with open(snapshot_file, 'w') as f: json.dump(cls.snapshot_state, f, indent=2)
        

    @classmethod
    def restore_from_snapshot(cls):
        if os.path.exists(os.path.join(cls.snapshot_dir, "snapshot_current.json")):
            snapshot_file = os.path.join(cls.snapshot_dir, "snapshot_current.json")
        else:
            snapshot_file = os.path.join(cls.snapshot_dir, "snapshot_previous.json")
        
        if not os.path.exists(snapshot_file):
            return False
        
        with open(snapshot_file, 'r') as f:
            snapshot_data = json.load(f)
        
        coordinator_state = snapshot_data['coordinator']
        
        # Restore pending tasks
        cls.pending_tasks = []
        for task_data in coordinator_state['pending_tasks']:
            task = coordinator_worker_task.Task(task_data['task_id'], task_data['filename'])
            cls.pending_tasks.append(task)
        
        cls.task_counter = coordinator_state['task_counter']

        cls.completed_tasks = []
        for task_data in coordinator_state['completed_tasks']:
            task = coordinator_worker_task(task_data['task_id'], task_data['filename'])
            cls.completed_tasks.append(task)
        
        cls.buffer_writer = []
        for prime in coordinator_state['buffer_writer']:
            cls.buffer_writer.append(prime)
        
        return True
    
    @classmethod
    def makeAfsClient(cls, ClientId, cacheDir, replicaAddrs):
        cls.afsclient = afsclient.AFSClient(ClientId, cacheDir, replicaAddrs)

# think of this as a unit of work that worker will invoke


# start the new coordinator function
# indefinitely serve requests at port 5000
# port 5000 is arbitiary. Can include in args if needed
async def newCoordinator(host='localhost', port=5000):
    loop = asyncio.get_running_loop()
    server = await loop.create_server(lambda: CoordinatorProtocol(),host, port)
    
    async with server:
        await server.serve_forever()

# take snapshots every 30 seconds.
# 30 is arbitiary number, we can tune/arg pass it if needed
async def periodicSnapshot(interval=30):
    while True:
        await asyncio.sleep(interval)
        await CoordinatorProtocol.initiateSnapshot()


async def main():

    if len(sys.argv) > 1:
        clientId = sys.argv[1]
    else:
        clientId = "coordinator"

    # pull the replica addresses
    if len(sys.argv) > 2:
        serversAddrs = sys.argv[2].split(',')
    else:
        serversAddrs = ["localhost:8080"]
    
    cacheDir = "/tmp/afs-{clientId}"

    CoordinatorProtocol.makeAfsClient(clientId, cacheDir, serversAddrs)
    if not CoordinatorProtocol.restore_from_snapshot():
        test_files = ["input_dataset_001.txt", "input_dataset_002.txt", "input_dataset_003.txt"]
        CoordinatorProtocol.loadFiles(test_files)
    
    # this kicks of a new bunch of tasks in the main event loop.
    coordinatorTask = asyncio.create_task(newCoordinator())
    bufferFlusher = asyncio.create_task(CoordinatorProtocol.flushBuffer())
    snapshotTask = asyncio.create_task(periodicSnapshot(interval=30))
    
    await asyncio.gather(coordinatorTask, bufferFlusher, snapshotTask)


if __name__ == "__main__":
    asyncio.run(main())