import asyncio
import json
import sys
import random
import time
import afsclient
from prime_finder import PrimeFinder



# worker finds prime numbers
# it is also repsonsible for implementing the snapshots
class WorkerClient(object):
    def __init__(self, workerID, serverAddrs, k=5):
        self.workerID = workerID
        self.reader = None # this is referring to StreamReader
        self.writer = None # this is referring to StreamWriter
        self.running = False
        self.tasks_processed = 0
        self.tasks_failed = 0
        self.buffer = b"" # empty byte string
        self.pf = PrimeFinder()  # Ferma iterations
        self.restart = False
        
        # requirement for caching files on disk
        # won't be stored in the cwd
        cacheDir = f"/tmp/afs-{workerID}"
        self.client = afsclient.AFSClient(workerID, cacheDir, serverAddrs)
        
        # Snapshot state
        self.snapshotState = {}

    async def run(self):
        try:
            await self.connect()
            while self.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            print(f"\n{self.workerID} shutting down.")
        except Exception as e:
            print(f"Error: {e}")
            self.restart = True
        finally:
            await self.disconnect()
            print(f"{self.workerID} stopped")
        if self.restart:
            self.restart = False
            await self.run()

    # connect to the running coordinator
    async def connect(self, host='localhost', port=5000):
        try:
            self.reader, self.writer = await asyncio.open_connection(host, port)
            self.running = True
            
            # Start background tasks
            asyncio.create_task(self.handle_messages())
            asyncio.create_task(self.send_heartbeat())
            
        except Exception as e:
            print(f"Connection failed: {e}")
            raise

    async def disconnect(self):
        self.running = False
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def handle_messages(self):
        while self.running:
            try:
                # pull just 4KB worth of data from network pipe
                # you can pull more, i just wrote 4KB for now
                data = await self.reader.read(4096)
                if not data:
                    print("Connection closed by coordinator")
                    self.running = False
                    self.restart= True
                    break
                
                self.buffer += data

                # keep reading from pipe until newspace is encountered
                while b'\n' in self.buffer:
                    line, self.buffer = self.buffer.split(b'\n', 1)
                    try:
                        message = json.loads(line.decode())
                        await self.msgRouter(message)
                    except (json.JSONDecodeError, Exception) as e:
                        print(f"error processing message: {e}")
                        
            except Exception as e:
                print(f"error in handle_messages: {e}")
                self.running = False
                break

    # this is a message router. it performs certain ops based on type of message
    # think of this as mux router if I wrote this in go
    async def msgRouter(self, message):
        msgType = message.get('type')

        match msgType:
            case 'request_id':
                await self.requestIDHandler(message)
            case 'task_assignment':
                await self.taskAssignmentHandler(message)

    # respond to coordinator's request to send the worker id
    async def requestIDHandler(self, message):
        await self.send_message({'type': 'register','worker_id': self.workerID})

    async def taskAssignmentHandler(self, message):
        task_id = message.get('task_id')
        filename = message.get('filename')
        
        # Process file asynchronously
        asyncio.create_task(self.process_file(task_id, filename))


    async def process_file(self, task_id, filename):
        try:
            # Open file via AFS (with caching)
            bytesResp = await self.client.open(filename)
            
            # Decode and parse
            content = bytesResp.decode('utf-8')
            
            if not content.strip():
                raise Exception(f"File is empty: {filename}")
        
            numbers = []
            for line in content.split('\n'):
                line = line.strip()
                if line:
                    try:
                        num = int(line)
                        numbers.append(num)
                    except ValueError:
                        pass
            
            # Find primes using Fermat
            primes = []
            for num in numbers:
                if self.pf.is_prime(num):
                    primes.append(num)
                        
            # Send results to coordinator
            await self.send_message({
                'type': 'primes_result',
                'task_id': task_id,
                'filename': filename,
                'primes': primes,
                'worker_id': self.workerID
            })
            
            self.tasks_processed += 1
            
            # Mark task complete
            await self.send_message({
                'type': 'task_complete',
                'task_id': task_id,
                'result': f'Found {len(primes)} primes'
            })
            
            # Close file (flushes if modified)
            await self.client.close(filename)
            print("Complete")
            
        except Exception as e:
            self.tasks_failed += 1
            
            await self.send_message({
                'type': 'task_failed',
                'task_id': task_id,
                'error': str(e)
            })

    async def send_message(self, message):
        try:
            data = json.dumps(message) + '\n'
            self.writer.write(data.encode())
            await self.writer.drain()
        except Exception as e:
            print(f"Error sending message: {e}")

    # show face to the professor?
    # send hearbeat msgs to the coordinator to let it know you're alive
    async def send_heartbeat(self):
        while self.running:
            await asyncio.sleep(5)
            if self.running:
                await self.send_message({'type': 'heartbeat','worker_id': self.workerID})

async def main():
    # pull the worker-id
    if len(sys.argv) > 1:
        workerID = sys.argv[1]
    else:
        # TODO increase vairance,
        # too much posibility for conflict with worker ID
        # Posibly pull from env varialbe
        workerID = f"worker-{random.randint(1000, 9999)}"

    # pull the replica addresses
    if len(sys.argv) > 2:
        serversAddrs = sys.argv[2].split(',')
    else:
        serversAddrs = ["localhost:8080"]
    
    # pull the ferma's number
    k = int(sys.argv[3]) if len(sys.argv) > 3 else 5 

    worker = WorkerClient(workerID, serversAddrs, k)

    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())