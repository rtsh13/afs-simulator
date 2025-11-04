import socket
import threading
import json
import os
import time
from collections import defaultdict

class Coordinator:
    def __init__(self, host='localhost', port=5000, input_dir='input_dataset.txt', output_file='./primes.txt'):
        self.host = host
        self.port = port
        self.input_dir = input_dir
        self.output_file = output_file

        #work queue
        self.available_files = []
        self.assigned_files = {}
        self.completed_files = set()
        self.lock = threading.Lock()

        #worker management
        self.workers = {}

        #snapshot (help me!!!)
        self.snapshot_in_progress = False
        self.snapshot_id = 0
        self.snapshot_state = {}
        self.workers_snapshot_received = set()
        self._init_files()

    def _init_files(self):
        if not os.path.exists(self.input_dir):
            os.makedirs(self.input_dir)
        self.available_files = [f for f in os.listdir(self.input_dir)
                                 if os.path.isfile(os.path.join(self.input_dir, f))]
        open(self.output_file, "a").close() 
    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"Coordinator listening on {self.host}:{self.port}, {len(self.available_files)} files to process.")
        try:
            while True:
                client_sock, addr = server.accept()
                threading.Thread(target=self._handle_worker, args=(client_sock, addr), daemon=True).start()
        except KeyboardInterrupt:
            print("Shutting down coordinator.")
        finally:
            server.close()
    def _handle_worker(self, client_sock, addr):
        worker_id = None
        try:
            while True:
                data = client_sock.recv(4096).decode()
                if not data:
                    break
                message = json.loads(data)
                msg_type = message.get('type') 

                if msg_type == 'register':
                    worker_id = message['worker_id']
                    with self.lock:
                        self.workers[worker_id] = client_sock
                    self._send(client_sock, {'type': 'ack', 'message': f'Worker {worker_id} registered.'})
                elif msg_type == 'request_work':
                    worker_id = message['worker_id']
                    work = self._assign_work(worker_id)
                    self._send(client_sock, work)
                elif msg_type == 'submit_primes':
                    primes = message['primes']
                    worker_id = message['worker_id']
                    self._write_primes(primes)
                    print(f"coordinator: Received {len(primes)} primes from worker {worker_id}")
                elif msg_type == 'work_done':
                    self._complete_file(message['file_name'], message['worker_id'])
        except Exception as e:
            print(f"Error handling worker {worker_id} at {addr}: {e}")
        finally:
            if worker_id:
                with self.lock:
                    if worker_id in self.workers:
                        del self.workers[worker_id]
                    self._reassign_work(worker_id)
            client_sock.close()
    def _assign_work(self, worker_id):
        with self.lock:
            if self.available_files:
                file_name = self.available_files.pop(0)
                self.assigned_files[file_name] = worker_id
                filepath = os.path.join(self.input_dir, file_name)
                print(f"Assigned file {file_name} to worker {worker_id}")
                return {'type': 'work', 'file_name': file_name, 'filepath': filepath}
            else:
                return {'type': 'no_work'}
    def _complete_file(self, file_name, worker_id):
        with self.lock:
            if file_name in self.assigned_files:
                del self.assigned_files[file_name]
            self.completed_files.add(file_name)
            print(f"Worker {worker_id} completed file {file_name}")
            print(f"Progress: {len(self.completed_files)}/{len(self.completed_files) + len(self.available_files) + len(self.assigned_files)} files completed.")
    def _reassign_file(self, worker_id):
        with self.lock:
            to_reassign = [f for f, w in self.assigned_files.items() if w == worker_id]
            for f in to_reassign:
                del self.assigned_files[f]
                self.available_files.append(f)
                print(f"Reassigned file {f} from worker {worker_id} back to available pool.")
    def _write_primes(self, primes):
        with self.lock:
            with open(self.output_file, 'a') as f:
                for prime in primes:
                    f.write(f"{prime}\n")
    def _send(self, client_sock, message):
        client_sock.sendall((json.dumps(message) + '\n').encode())

    def initiate_snapshot(self):
        pass  
        # need help, not sure.