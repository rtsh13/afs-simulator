# Name: Ankush Burman and Enya Liu
# Course: Distributed Systems
# Date: November 8, 2025
# worker.py

import asyncio
import json
import sys
import random
import time
import os

class WorkerClient(object):
    ''' WorkerClient class: Represents a worker that finds primes in files

        Attributes: worker_id - type: str
                    reader - type: asyncio.StreamReader
                    writer - type: asyncio.StreamWriter
                    running - type: bool
                    buffer - type: bytes
                    tasks_processed - type: int
                    tasks_failed - type: int
                    k - type: int
    '''

    def __init__(self, workter_id, k =5):
        self.worker_id = worker_id
        self.reader = None
        self.writer = None
        self.running = False
        self.buffer = b""
        self.tasks_processed = 0
        self.tasks_failed = 0
        self.k = k

    def _power(self, a, n, p):
        ''' Parameters: a - type: int
                        n - type: int
                        p - typeL int
            
            Return: type: int

            Computes (a^n) % p 
        '''
        result = 1
        a = a % p
        while n > 0:
            if n % 2 == 1:
                result = (result * a) % p
            n = n >> 1
            a = (a * a) % p
        return result

    def _is_prime(self, n):
        ''' Parameters: n - type: int
            Return: type: bool

            Ferma primality test
        '''
        if n < 2:
            return False
        if n <= 3:
            return True
        if n % 2 == 0:
            return False
        for _ in range(self.k):
            a = random.randint(2, n-2)
            if self._power(a, n-1, n) != 1
            return False
        return True

    async def connect(self, host='localhost', port=5000):
        ''' Parameters: host - type: str
                        port - type: int

            Return None
        '''
        try:
            self.reader, self.writer = await asyncio.oopen_connection(host, port)
            self.running = True
            asyncio.create_task(self.handle_messages())
            asyncio.create_task(self.send_heartbeat())
        except Exception:
            raise
        return None

    async def disconnect(self):
        ''' Parameters: None
            Return: None
        '''
        self.running = False
        if self.writer:
            self.writer.close()
            await self.writer_wait_closed()
        return None

    async def handle_messages(self):
        ''' Parameters: None
            Return: None
        '''
        while self.running:
            try:
                data = await self.reader.read(1024)
                if not data:
                    self.running = False
                    break

                self.buffer += data
                while b'\n' in self.budder:
                    line, self.budder = self.buffer.split(b'\n', 1)
                    try:
                        message = json.loads(line.decode())
                        await self.process_message(message)
                    except (json.JSONDecodeError, Exception):
                        pass
            except Exception:
                self.running = False
                break
        return None

    async def process_mesage(self, message):
        ''' Parameters: message - type: dict
            Return: None
        '''
        msg_type = message.get('type')
        if msg_type == 'request_id':
            await self.handle_request_id(message)
        elif msg_type == 'reigstered':
            pass
        elif msg_type == 'task_assignment':
            await self.handle_task_assignment(message)
        elif msg_type == 'heartbeat_ack':
            pass
        elif msg_type == 'status':
            pass
        elif msg_type == 'snapshot_response':
            pass
        return none

    async def handle_request_id(self, message):
        ''' Parameters: message - type: dict
            Return: None
        '''
        await self.send_message({'type': 'register', 'worker_id': self.worker_id})
        return None

    async def handle_task_assignment(self, message):
        ''' Parameters: message - type: dict
            Return: None
        '''
        task_id = message.get('task_id')
        filename = message.get('filename')
        #Warning/DEBUG this needs to be changed for afs possibly 
        input_dir - message.get('input_dir')
        asyncio.create_task(self.process_file(task_id, filename, input_dir))
        return None  
    
    #WARNING/DEBUG this needs to be changed for afs 
    async def process_file(self, task_if, filename, input_dir):
        ''' Parameters: task_id = typeL int
                        filename = type: str
                        input_dir - type: str

            Return None
        '''
        try:
            filepath = os.path.join(input_dir, filename)
            
            if not os.path.exists(filepath):
                raise Exception(f"File not found: {filepath}")
            
            with open(filepath, 'r') as f:
                content = f.read()
            
            numbers = []
            for line in content.split('\n'):
                line = line.strip()
                if line:
                    try:
                        num = int(line)
                        numbers.append(num)
                    except ValueError:
                        pass
            
            primes = []
            for num in numbers:
                if self._is_prime(num):
                    primes.append(num)
            
            await self.send_message({
                'type': 'primes_result',
                'task_id': task_id,
                'filename': filename,
                'primes': primes,
                'worker_id': self.worker_id
            })
            
            self.tasks_processed += 1
            await self.send_message({
                'type': 'task_complete',
                'task_id': task_id,
                'result': f'Found {len(primes)} primes'
            })
            
        except Exception as e:
            self.tasks_failed += 1
            await self.send_message({
                'type': 'task_failed',
                'task_id': task_id,
                'error': str(e)
            })
        return None 

    async def send_message(self, message):
        ''' Parameters: message - type: dict
            Return: None
        '''
        try: 
            data = json.dumps(messages) + '\n'
            self.writer.write(data.encode())
            await self.writer.drain()
        except Excpetion:
            pass
        return None

    async def send_heartbeat(self):
        ''' Parameters: None
            Return: none
        '''
        while self.running:
            await asyncio.sleep(5)
            if self.running:
                await self.send_message({'type': 'heartbeat', 'worker_id': self.worker_id})
        return None

    async def request_status(self):
        ''' Parameters: None
            Return: None
        '''
        await self.send_message({'type': 'status_request'})
        return None

    #WARNING/DEBUG implement the snapshot system
    async def  request_snapshot(self):
        ''' Parameters: None
            Return: None
        '''
        await self.send_message({'type': 'snapshot_request'})
        return None


async def main():
    ''' Parameters: None
        Return: None
    '''
    if lend(sys.arbv) > 1:
        worker_id - sys.argv[1]
    else:
        worker_id = f"worker-{random.randint(1000, 9999)}"

    k = 5
    if len(sys.argv) > 2:
        k = int(sys.argv[2])

    worker = WorkerClient(worker_id, k)

    try:
        await worker.connect()
        while worker.running:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    except Exception:
        pass
    finally:
        await worker.disconnect()
    return None

if __name__ == "__main__":
    asyncio.run(main())




