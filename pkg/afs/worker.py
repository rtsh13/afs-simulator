import asyncio
import json
import sys
import random
import time
import os
import base64

class AFSJsonRPCClient:
    """A simplified client for Go's net/rpc running JSON-RPC over TCP."""

    def __init__(self, server_addr="localhost:8080"):
        # The default AFS server address from your start_system.sh is not provided, 
        # but the Go server typically runs on 8080.
        host, port = server_addr.split(':')
        self.host = host
        self.port = int(port)
        self.client_id = "" # Will be set by WorkerClient

    async def _rpc_call(self, method_name: str, params: dict):
        """Generic JSON-RPC call wrapper."""
        try:
            # Establish a new TCP connection for each RPC call
            reader, writer = await asyncio.open_connection(self.host, self.port)
            
            # JSON-RPC Request structure (note: params is a list containing the request object)
            request = {
                "method": method_name,
                "params": [params],
                "id": random.randint(1, 10000) # Use a random ID
            }

            json_request = json.dumps(request)
            writer.write(json_request.encode('utf-8'))
            await writer.drain()

            # Read the full response
            data = await reader.read(4096 * 10) # Read a large buffer since file content can be large
            writer.close()
            await writer.wait_closed()
            
            if not data:
                raise Exception("Empty response from AFS server.")

            response = json.loads(data.decode('utf-8'))
            
            # Check for server-side RPC errors
            if response.get("error"):
                raise Exception(response["error"].get("message", "Unknown RPC Error"))
            
            # Check for application-level errors (Success=false)
            result = response.get("result")
            if not result or not result.get('Success', True): 
                 raise Exception(result.get('Error', 'File operation failed'))

            return result
            
        except ConnectionRefusedError:
            raise Exception(f"AFS Server connection refused at {self.host}:{self.port}. Is the server running (with JSON-RPC)?")
        except Exception as e:
            raise Exception(f"AFS RPC Error ({method_name}): {e}")

    async def fetch_file(self, client_id, filename):
        """Corresponds to FileServer.FetchFile RPC"""
        req_params = {
            "ClientID": client_id,
            "Filename": filename,
        }
        
        # The method name must match the Go struct and method: "FileServer.FetchFile"
        result = await self._rpc_call("FileServer.FetchFile", req_params)
        
        # The Go []byte (Content) will be base64 encoded by JSON-RPC, so we must decode it.
        content_bytes = base64.b64decode(result['Content'])
        
        return content_bytes

class WorkerClient(object):
    """WorkerClient class: Represents a worker that finds primes in files"""

    def __init__(self, worker_id, k=5):
        self.worker_id = worker_id
        self.reader = None
        self.writer = None
        self.running = False
        self.buffer = b""
        self.tasks_processed = 0
        self.tasks_failed = 0
        self.k = k  # Number of iterations for Miller-Rabin
        
        # Snapshot state
        self.snapshot_state = {}
        self.recording_channels = {}
        
        # AFS Client Initialization 
        self.afs_client = AFSJsonRPCClient("localhost:8080")
        self.afs_client.client_id = worker_id # Set client ID for RPC requests


    def _power(self, a, n, p):
        """Compute (a^n) % p using fast exponentiation"""
        result = 1
        a = a % p
        while n > 0:
            if n % 2 == 1:
                result = (result * a) % p
            n = n >> 1
            a = (a * a) % p
        return result

    def _miller_rabin(self, n):
        """Miller-Rabin primality test - more accurate than Fermat"""
        if n < 2:
            return False
        if n == 2 or n == 3:
            return True
        if n % 2 == 0:
            return False
        
        # Write n-1 as 2^r * d
        r, d = 0, n - 1
        while d % 2 == 0:
            r += 1
            d //= 2
        
        # Witness loop
        for _ in range(self.k):
            a = random.randint(2, n - 2)
            x = self._power(a, d, n)
            
            if x == 1 or x == n - 1:
                continue
            
            for _ in range(r - 1):
                x = self._power(x, 2, n)
                if x == n - 1:
                    break
            else:
                return False
        
        return True

    def _is_prime(self, n):
        """Check if n is prime using Miller-Rabin"""
        return self._miller_rabin(n)
    
    
    async def connect(self, host='localhost', port=5000):
        """Connect to coordinator"""
        try:
            self.reader, self.writer = await asyncio.open_connection(host, port)
            self.running = True
            
            print(f"Worker {self.worker_id} connected to {host}:{port}")
            
            # Start background tasks
            asyncio.create_task(self.handle_messages())
            asyncio.create_task(self.send_heartbeat())
            
        except Exception as e:
            print(f"Connection failed: {e}")
            raise

    async def disconnect(self):
        """Disconnect from coordinator"""
        self.running = False
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def handle_messages(self):
        """Handle incoming messages from coordinator"""
        while self.running:
            try:
                data = await self.reader.read(4096)
                if not data:
                    print("Connection closed by coordinator")
                    self.running = False
                    break

                self.buffer += data
                while b'\n' in self.buffer:
                    line, self.buffer = self.buffer.split(b'\n', 1)
                    try:
                        message = json.loads(line.decode())
                        await self.process_message(message)
                    except (json.JSONDecodeError, Exception) as e:
                        print(f"Error processing message: {e}")
                        
            except Exception as e:
                print(f"Error in handle_messages: {e}")
                self.running = False
                break

    async def process_message(self, message):
        """Route messages to appropriate handlers"""
        msg_type = message.get('type')
        
        if msg_type == 'request_id':
            await self.handle_request_id(message)
        elif msg_type == 'registered':
            print(f"Successfully registered as {self.worker_id}")
        elif msg_type == 'task_assignment':
            await self.handle_task_assignment(message)
        elif msg_type == 'heartbeat_ack':
            pass  # Heartbeat acknowledged
        elif msg_type == 'snapshot_marker':
            await self.handle_snapshot_marker(message)
        elif msg_type == 'status':
            print(f"Status: {message}")

    async def handle_request_id(self, message):
        """Respond to ID request from coordinator"""
        await self.send_message({
            'type': 'register',
            'worker_id': self.worker_id
        })

    async def handle_task_assignment(self, message):
        """Handle new task assignment"""
        task_id = message.get('task_id')
        filename = message.get('filename')
        
        print(f"Received task {task_id}: process file {filename}")
        
        # Process file asynchronously
        asyncio.create_task(self.process_file(task_id, filename))
        
    async def handle_snapshot_marker(self, message):
        """Handle snapshot marker (Chandy-Lamport algorithm)"""
        snapshot_id = message.get('snapshot_id')
        
        print(f"Received snapshot marker: {snapshot_id}")
        
        # Save local state
        state = {
            'worker_id': self.worker_id,
            'tasks_processed': self.tasks_processed,
            'tasks_failed': self.tasks_failed,
            'timestamp': time.time()
        }
        
        # Send state to coordinator
        await self.send_message({
            'type': 'snapshot_state',
            'snapshot_id': snapshot_id,
            'worker_id': self.worker_id,
            'state': state
        })

    async def process_file(self, task_id, filename):
        """
        Process a file to find prime numbers.
        Now fetches file content via RPC from the AFS server.
        """
        try:
            # --- AFS INTEGRATION: Fetch file content via RPC ---
            print(f"Fetching file {filename} from AFS ({self.afs_client.host}:{self.afs_client.port})...")
            
            # 1. Fetch the whole file content over RPC
            content_bytes = await self.afs_client.fetch_file(self.worker_id, filename)
            
            # 2. Decode content (assuming the files contain numbers as plain text)
            content = content_bytes.decode('utf-8')
            
            # Check for empty content (e.g., if fetch was successful but file was empty)
            if not content.strip():
                 raise Exception(f"File content is empty or malformed: {filename}")
            
            # Parse numbers
            numbers = []
            for line in content.split('\n'):
                line = line.strip()
                if line:
                    try:
                        # Ensure numbers are 64-bit unsigned integers (Python handles large integers)
                        num = int(line)
                        numbers.append(num)
                    except ValueError:
                        pass
            
            print(f"Processing {len(numbers)} numbers from {filename}")
            
            # Find primes
            primes = []
            for num in numbers:
                # Use Miller-Rabin primality test
                if self._is_prime(num):
                    primes.append(num)
            
            # Send results to the coordinator
            await self.send_message({
                'type': 'primes_result',
                'task_id': task_id,
                'filename': filename,
                'primes': primes,
                'worker_id': self.worker_id
            })
            
            self.tasks_processed += 1
            
            # Mark task complete
            await self.send_message({
                'type': 'task_complete',
                'task_id': task_id,
                'result': f'Found {len(primes)} primes out of {len(numbers)} numbers'
            })
            
            print(f"Task {task_id} complete: found {len(primes)} primes")
            
        except Exception as e:
            print(f"Task {task_id} failed: {e}")
            self.tasks_failed += 1
            
            await self.send_message({
                'type': 'task_failed',
                'task_id': task_id,
                'error': str(e)
            })

    async def send_message(self, message):
        """Send message to coordinator"""
        try:
            data = json.dumps(message) + '\n'
            self.writer.write(data.encode())
            await self.writer.drain()
        except Exception as e:
            print(f"Error sending message: {e}")

    async def send_heartbeat(self):
        """Send periodic heartbeat to coordinator"""
        while self.running:
            await asyncio.sleep(5)
            if self.running:
                await self.send_message({
                    'type': 'heartbeat',
                    'worker_id': self.worker_id
                })

    async def request_status(self):
        """Request status from coordinator"""
        await self.send_message({'type': 'status_request'})


async def main():
    if len(sys.argv) > 1:
        worker_id = sys.argv[1]
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
        print("worker shutdown")
    except Exception as e:
        print(f"error: {e}")
    finally:
        await worker.disconnect()


if __name__ == "__main__":
    asyncio.run(main())