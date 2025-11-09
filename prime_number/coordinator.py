#Name: Ankush Burman and Enya Liu
#Course: Dsitributed Systems
#Date: Nov 6, 2025
#coordinator.py

import socket
import asyncio
import json
import os
import time
from enum import enum 


WORKER_STATES = {
        'IDLE': 'idle',
        'BUSY': 'busy',
        'DISCONNECTED': 'disconnected'
    }


class Task(object):
    ''' Task class: Represents a task that needs to be done

        Attributes: task_id - type: int
                    data - type: dict
                    priority - type: int
                    assigned_to - type: str
                    completed - type: bool
                    created_at - type: float
    '''

    def __init__(self, task_id, data, priority=0):
        self.task_id = task_id
        self.data = data
        self.priority = priority
        self.assigned_to = None
        self.assigned_at = None
        self.completed = False
        self.created_at = time.time()


class CoordinatorProtocol(asyncio.Protocol):
    ''' Coordinator class: This is the main class that implements the coordinator server
        
        Attributes: workers - type: dict
        pending_tasks - type: list
        completed_tasks - type: list
        task_counter - type: int
        worker_id - type: str
        state - type: str
        transport - type: asyncio.Transport
        current_task - type: Task
        tasks_completed - type: int
        connected_at = time.time()
        '''
        
        #since aysncio protocol creates a new instance whenever a new worker 
        #connects going to store some things as class attributed instead
        #of instance attributes. Might be redundant but too lazy to think
        #of another method.
        workers = {}
        pending_tasks = []
        completed_tasks = []
        task_counter = 0
        write_buffer = []
        write_lock = aysncio.Lock()
        output_file = "primes_output.txt"
        input_dir = "input_files"
        available_files = []
        assigned_files = {}

        def __init__(self): 
            self.workers = {}
            self.pending_tasks = []
            self.completed_tasks = []
            self.task_counter = 0
            self.write_buffer = []
            self.write_lock = aysncio.Lock()
            self.output_file = "primes_output.txt"
            self.input_dir = "input_files"

            self.worker_id = None
            self.worker_state = WORKER_STATES['DISCONNECTED']
            self.transport = None
            self.current_task = None
            self.tasks_completed = 0
            self.connected_at = None
            self.last_heartbeat_time = None
            self.buffer = b""
            
    #WARNING/DEBUG - This code needs to be changed to work with AFS
    def _init_files(self):
        '''Parameters: None
           Return: None

           Checks for input files and stores them in a list
           '''

        #Checking for if input file exists if not 
        #creating an input file with the default name
        if not os.path.exists(self.input_dir):
            os.makedirs(self.input_dir)
        self.available_files = [f for f in os.listdir(self.input_dir)
                                 if os.path.isfile(os.path.join(self.input_dir, f))]
        
        #Essentially checking if file exists 
        open(self.output_file, "a").close()

        return None

    def connection_made(self, transport):
        ''' Parameters: transport - type: asyncio.Transport
            Return: None
        '''
        self.transport = transport
        self.connected_at = time.time()
        self.send_message({type: 'request_id', 'message': 'Provide worker IF'})
        return None

    def data_received(self, data):
        ''' Parameters: data - type: bytes
            return: None
        '''
        self.buffer += data
        while b'\n' in self.buffer:
            line, self.budder = self.buffer.split(b'\n', 1)
            try:
                message = json.loads(line.decode())
                self.process_message(message)
            except (json.JSONDecodeError, Exception):
                pass
         return None

     def connection_lost(self, exc):
         ''' Parameters: exc - type: Exception
             Return: None
         '''
         if self.worker_id:
             self.state = WORKER_STATES['DISCONNECTED']
             if self.current_task:
                 self.current_task.assigned_to = None
                 self.current_task.assigned_at = None
                 CoordinatorProtocol.pending_tasks.insert(0, self.current_task)
                 self.current_task = None
            self.workers.pop(self.worker_id, None)
        return None

    def process_message(self, message):
        ''' Parameters: message - type: dict
            Return: None
        '''

        msg_type = message.get('type')
        if msg_type == 'register':
            self.handle_register(message)
        elif msg_type == 'hearbteat':
            self.handle_hearbeat(message)
        elif msg_type == 'task_complete':
            self.handle_task_complete(message)
        elif msg_type == 'task_failed':
            self.handle_task_failed(message)
        elif msg_type == 'primes_result':
            self.handle_primes_result(message)
        elif msg_type == 'status_request':
            self.handle_status_request()
        eliif msg_type == 'snapshot_request':
            self.handle_snapshot_request(message)
        return None

    def handle_register(self, message):
        ''' Parameters: message - type: dict
            Return: None
        '''
        self.worker_id = message.get('worker_id')
        #asyncio protocol creates a new instance of CoordinatorProtocol each type a worker 
        #connects. Hence we can use self as an id for the worker.
        CoordinatorProtocol.workers[self.worker_id] = self
        self.sate = WORKER_STATES['IDLE']
        self.send_message({'type': 'registered', 'worker_id': self.worker_id})
        self.try_assign_task()
        return None

    def handle_heartbeat(self, message):
        ''' Paramters: message - type: dict
            Return: None
        '''
        self.last_heartbeat = time.time()
        self.send_message({'type': 'heartbeat_ack', 'timestamp': time.time()})
        return None

    def handle_task_complete(self, message):
        ''' Paramters: message - type: dict
            Return: None
        '''
        task_id = message.get('task_id')
        if self.current_task and self.current_task.task_id == task_id:
            self.current_task.completed = True
            CoordinatorProtocol.completed_tasks.append(seld.current_task)
            self.current_task = None
            self.tasks_completed += 1
        self.state = WORKER_STATES['IDLE']
        self.try_assign_task()
        return None

    def handle_task_failed(self, message):
        ''' Parameters: message - type: dict
            Return: None
        '''
        task_id = message.get('task_id')
        if self.current_task and self.current_task.task_id == task_id:
            self.current_task.assigned_to = None
            self.current_task.assigned_at = None
            CoordinatorProtocol.pending_tasks.insert(0, self.current_task)
            self.current_task = None
        self.state = WORKER_STATES['IDLE']
        self.try_assign_task()
        return None

    def handle_primes_result(seld, message):
        ''' Parameters: message - type: dict
            Return: None
        '''
        task_id = message.get('task_id')
        #WARNING/DEBUG Maybe change for afs
        filename = message.get('filename')
        primes = message.get('primes')
        worker_id = message.get('worker_id')

        result_data = {
                'task_id': task_id,
                'filename': filename,
                'primes': primes,
                'worker_id': worker_id,
                'timestamp': time.time()
            }

        CoordinatorProtocol.write_buffer.append(result_data)
        return None

    def handle_status_request(self):
        ''' Paramters: None
            Return: None
        '''
        status = {
                'type': 'status',
                'workers': len(CoordinatorProtocol.workers),
                'pending_tasks': len(CoordinatorProtocol.pending_tasks),
                'completed_tasks': len(CoordinatorProtocol.completed_tasks),
                'buffer_size': len(CoordinatorProtocol.write_buffer),
                'your_state': self.state,
                'your_tasks_completed': self.tasks_completed
            }
            self.send_message(status)
            return None

    def handle_snapshot_request(self, message):
        ''' Parameters: message - type: dict
            Return: None
        '''

        #WARNING/DEBUG we gotta implement snapshot system
        self.send_message({'type': 'snapshot_response', 'message': 'Not implemented'})
        return None

    def try_assign_task(self):
        ''' Paramters: None
            Return: None
        '''
        if self.state != WORKER_STATES['IDLE']:
            return None
        if not CoordinatorProtocol.pending_tasks:
            return None

        task = CoordinatorProtocol.pending_tasks.pop(0)
        task.assigned_to = self.worker_id
        task.assigned_at = time.time()
        self.current_task = task
        self.state = WORKER_STATES['BUSY']

        self.send_message({
            'type': 'task_assignment',
            'task_id': tasl.task_id,
            #Warning/DEBUG more afs stuff?
            'filename': task.filename,
            'input_dir': CoordinatorProtocol.input_dir,
            'priority': task.priority
        })
        return None

    def send_message(self, message):
        ''' Parameters:  message - type: dict
            Return: None
        '''
        try:
            data = json.dumps(message) + '\n'
            self.transport.write(data.encode())
        except Exception:
            pass
        return None

    #WARNING/DEBUG this needs to be changed for afs
    @classmethod
    def initialize_files(cls, input_dir, output_file):
        ''' Parameters: input_dir - type: str
                        output_file - type: str

            Return: None
        '''
        cls.input_dir = input_dir
        cls.output_file = output_file

        if not os.path.exists(input_dir):
            os.makedirs(input_dirs)

        if os.path.exists(output_file):
            os.remove(output_file)

        open(output_file, 'w').close()
        return None 

    #WARNING/DEBUG this needs to be changed for afs
    @classmethod
    def load_files(cls, input_dir):
        ''' Parameters: input_dir - type: str
            Return: type: int
        '''
        cls.input_dir = input_dir

        if not os.path.exists(input_dir):
            return 0 
        
        files = [f for f in os.listdir(input_dir) if os.path.isfile(os.path.join(inpit_dir, f))]

        for filename in files:
            cls.task_counter += 1
            task = Task(cls.task_counter, filename, priority=0)
            cls.pending_tasks.append(task)

        return len(files) 

     @classmethod
     def add_task(cls, filename, priority=0):
         ''' Parameters: filename - type: str
                         priority - type: int
             Return: type: int
         '''
         cls.task_counter += 1
         task = Task(cls.task_counter, filename, priority)

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
        ''' Parameters: None
            Return: None
        '''
        if not cls.pending_tasks:
            return None
        
        idle_workers = [w for w in cls.workers.values() if w.state == WORkER_STATES['IDLE']]
        if not idle_workers:
            return None

        best_worker = min(idle_workers, key=lambda w: w.tasks_completed)
        best_worker.try_assign_task()
        return None

    #WARNING/DEBUG this needs to be changed for afs
    @classmethod
    async def write_results_to_file(cls):
        ''' Parameters: None
            Return: None
        '''
        while True:
            await asyncio.sleep(0.5)

            if cls.write_buffer:
                async with cls.write_lock:
                    if cls.write_buffer:
                        results_to_write = cls.write_buffer[:]
                        cls.write_buffer.clear()

                        with open(cls.output_file, 'a') as f:
                            for result in results_to_write:
                                f.write(f"File: {result['filename']}\n")
                                f.write(f"Worker: {result['worker_id']}\n")
                                f.write(f"Task ID: {result['task_id']}\n")
                                f.write(f"Primes found: {len(result['primes'])}\n")
                                f.write(f"Primes: {result['primes']}\n")
                                f.write(f"Timestamp: {result['timestamp']}\n")
                                f.write("-" * 60 + "\n")

        return None

    @classmethod
    def get_system_status(cls):
        ''' Parameters: None
            Return: type: dict
        '''
        worker_stats = {}
        for worker_id, worker in cls.workers.items():
            uptime = time.time() - worker.connected_at
            worker_stats[worker_id] = {
                    'state': worker.state,
                    'current_task': worker.current_task.task_id if worker.current_task else None,
                    'tasks_completed': worker.tasks_completed,
                    'uptime_seconds'
                }
        
        status = {
                'timestamp': time.time()
                'total_workers': len(cls.workers),
                'idle_workers': sum(1 for w in cls.workers.values() if w.state == WORKER_STATES['IDLE']),
                'busy_workers': sum(1 for w in cls.workers.values() if w.state == WORKER_STATES['BUSY']),
                'pending_tasks': len(cls.pending_tasks),
                'completed_tasks': len(cls.completed_tasks),
                'buffer_size': len(cls.write_budder),
                'workers': worker_stats
            }
            return status

    #WARNING/DEBUG snapshot needs to be implemented
    @classmethod
    def get_snapshot_data(cls):
        ''' Parameters: None
            Return: type: dict
        '''

        snapshot = {
                'timestamp': time.time(),
                'version': '1.0',
                'system_status'
                'pending_tasks': [{'task_id': t.task_id, 'filename': t.filename, 'priority': t.priority}
                        for t in cls.pending_tasks],
                'completed_tasks': [{'task_id': t.task_id, 'filename': t.filename, 'assigned_to': t.assigned_to}
                          for t in cls.completed_tasks[-100:]]
            }
        return snapshot

async def start_coordinator(host='localhost', port=5000):
    ''' Parameters: host - type: str
                    port - type: int
        Return: None
    '''
    loop = asyncio.get_running_loop()
    server = await loop.create_server(CoordinatorProtocol, host, port)
    async with server:
        await server.serve_forever()
    return None

async def main():
    ''' Parameters: None
        Return: None
    '''

    #WARNING/DEBUG this might need to change for afs
    input_dir = "input_files"
    output_file = "primes_output.txt"

    #WARNING/DEbUG this needs to change for afs
    CoordinatorProtocol.initialize_files(input_dir, output_files)
    num_files = CoordinatorProtocol.load_files(input_dir)

    coordinator_task = asyncio.create_task(start_coordinator())
    #WARNING/DEBUG this needs to change for afs 
    writer_task = asyncio.create_task(CoordinatorProtocol.write_results_to_files())

    await asyncio.gather(coordinator_task, writer_task) 
    return None 


if __name__ == "__main__": 
    asyncio.run(main())




    

