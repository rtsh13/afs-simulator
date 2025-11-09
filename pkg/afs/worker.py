import asyncio
import json
import sys
import random
import time
import os
import base64
import hashlib


class AFSClient:
    def __init__(self, client_id, cache_dir, server_addr="localhost:8080"):
        """
        Initialize AFS client
        
        Args:
            client_id: Unique client identifier (e.g., "worker-1")
            cache_dir: Local directory for cached files (e.g., "/tmp/afs-worker-1")
            server_addr: AFS server address (e.g., "localhost:8080")
        """
        self.client_id = client_id
        self.cache_dir = cache_dir
        self.server_addr = server_addr
        
        # Parse server address
        host, port = server_addr.split(':')
        self.host = host
        self.port = int(port)
        
        # Cache metadata: filename -> CacheEntry
        self.cache = {}
        
        # Create cache directory
        os.makedirs(cache_dir, exist_ok=True)
        
        print(f"AFS Client {client_id} initialized")
        print(f"  Server: {server_addr}")
        print(f"  Cache dir: {cache_dir}")

    async def _rpc_call(self, method_name: str, params: dict):
        """Make JSON-RPC call to Go server"""
        try:
            reader, writer = await asyncio.open_connection(self.host, self.port)
            
            request = {
                "method": method_name,
                "params": [params],
                "id": random.randint(1, 100000)
            }

            json_request = json.dumps(request)
            writer.write(json_request.encode('utf-8'))
            await writer.drain()

            # Read response (large buffer for file content)
            data = await reader.read(10 * 1024 * 1024)  # 10MB
            writer.close()
            await writer.wait_closed()
            
            if not data:
                raise Exception("Empty response from server")

            response = json.loads(data.decode('utf-8'))
            
            if response.get("error"):
                raise Exception(f"RPC Error: {response['error'].get('message', 'Unknown')}")
            
            result = response.get("result")
            if not result:
                raise Exception("No result in RPC response")
                
            # Check application-level success
            if not result.get('Success', True):
                raise Exception(result.get('Error', 'Operation failed'))

            return result
            
        except ConnectionRefusedError:
            raise Exception(f"Cannot connect to AFS server at {self.host}:{self.port}")
        except Exception as e:
            raise Exception(f"RPC call failed ({method_name}): {e}")

    def _get_cache_path(self, filename):
        """Get local cache file path"""
        return os.path.join(self.cache_dir, filename)

    async def open(self, filename):
        """
        Open file (Task 1B: Caching)
        
        Process:
        1. Check if file is in cache
        2. If cached, validate with TestAuth
        3. If cache invalid or not cached, fetch from server
        4. Store in local cache
        """
        print(f"[AFS] Opening {filename}...")
        
        cache_path = self._get_cache_path(filename)
        
        # Check if file is in cache
        if filename in self.cache:
            print(f"[AFS] File {filename} found in cache, validating...")
            
            # Validate cache with TestAuth RPC
            cached_version = self.cache[filename]['version']
            
            auth_req = {
                "ClientID": self.client_id,
                "Filename": filename,
                "Version": cached_version
            }
            
            try:
                auth_resp = await self._rpc_call("FileServer.TestAuth", auth_req)
                
                if auth_resp.get('Valid'):
                    print(f"[AFS] Cache valid for {filename} (version {cached_version})")
                    # Read from local cache
                    with open(cache_path, 'rb') as f:
                        content = f.read()
                    return content
                else:
                    print(f"[AFS] Cache stale for {filename}, fetching fresh copy...")
            except Exception as e:
                print(f"[AFS] TestAuth failed: {e}, fetching fresh copy...")
        
        # Fetch from server
        print(f"[AFS] Fetching {filename} from server...")
        content = await self._fetch_from_server(filename)
        
        return content

    async def _fetch_from_server(self, filename):
        """
        Fetch entire file from server (Task 1A: RPC)
        Implements whole-file caching
        """
        fetch_req = {
            "ClientID": self.client_id,
            "Filename": filename
        }
        
        result = await self._rpc_call("FileServer.FetchFile", fetch_req)
        
        # Decode base64 content (Go []byte is base64-encoded in JSON)
        content_bytes = base64.b64decode(result['Content'])
        version = result['Version']
        
        # Store in local cache file
        cache_path = self._get_cache_path(filename)
        with open(cache_path, 'wb') as f:
            f.write(content_bytes)
        
        # Update cache metadata
        self.cache[filename] = {
            'version': version,
            'path': cache_path,
            'dirty': False,
            'size': len(content_bytes)
        }
        
        print(f"[AFS] Cached {filename} (version {version}, {len(content_bytes)} bytes)")
        
        return content_bytes

    async def write(self, filename, content):
        """
        Write to file (marks as dirty for flush on close)
        """
        cache_path = self._get_cache_path(filename)
        
        # Write to local cache
        with open(cache_path, 'wb') as f:
            f.write(content)
        
        # Mark as dirty
        if filename in self.cache:
            self.cache[filename]['dirty'] = True
        else:
            self.cache[filename] = {
                'version': 0,
                'path': cache_path,
                'dirty': True,
                'size': len(content)
            }
        
        print(f"[AFS] Wrote {len(content)} bytes to {filename} (dirty, will flush on close)")

    async def close(self, filename):
        """
        Close file (Task 1B: Flush on close if modified)
        
        If file was modified (dirty), flush to server
        """
        if filename not in self.cache:
            print(f"[AFS] File {filename} not open, nothing to close")
            return
        
        cache_entry = self.cache[filename]
        
        if cache_entry['dirty']:
            print(f"[AFS] Flushing dirty file {filename} to server...")
            await self._flush_to_server(filename)
        else:
            print(f"[AFS] File {filename} clean, no flush needed")
        
        # Keep in cache for future use, just mark as closed
        print(f"[AFS] Closed {filename}")

    async def _flush_to_server(self, filename):
        """Flush modified file back to server"""
        cache_path = self._get_cache_path(filename)
        
        # Read from local cache
        with open(cache_path, 'rb') as f:
            content = f.read()
        
        store_req = {
            "ClientID": self.client_id,
            "Filename": filename,
            "Content": base64.b64encode(content).decode('utf-8')  # Encode to base64
        }
        
        result = await self._rpc_call("FileServer.StoreFile", store_req)
        
        new_version = result['NewVersion']
        
        # Update cache metadata
        self.cache[filename]['version'] = new_version
        self.cache[filename]['dirty'] = False
        
        print(f"[AFS] Flushed {filename} to server (new version: {new_version})")

    async def create(self, filename):
        """Create new file on server"""
        create_req = {
            "ClientID": self.client_id,
            "Filename": filename
        }
        
        await self._rpc_call("FileServer.CreateFile", create_req)
        print(f"[AFS] Created file {filename} on server")

    def clear_cache(self):
        """Clear all cached files"""
        for filename in list(self.cache.keys()):
            cache_path = self._get_cache_path(filename)
            if os.path.exists(cache_path):
                os.remove(cache_path)
        self.cache.clear()
        print(f"[AFS] Cache cleared")


class WorkerClient(object):
    """
    Worker that finds prime numbers using AFS for file access
    Implements Chandy-Lamport snapshots (Task 5)
    """

    def __init__(self, worker_id, afs_server="localhost:8080", k=5):
        self.worker_id = worker_id
        self.reader = None
        self.writer = None
        self.running = False
        self.buffer = b""
        self.tasks_processed = 0
        self.tasks_failed = 0
        self.k = k  # Miller-Rabin iterations
        
        # Initialize AFS client with caching
        cache_dir = f"/tmp/afs-{worker_id}"
        self.afs_client = AFSClient(worker_id, cache_dir, afs_server)
        
        # Snapshot state
        self.snapshot_state = {}

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
        """Miller-Rabin primality test"""
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
        """Check if n is prime"""
        return self._miller_rabin(n)

    async def connect(self, host='localhost', port=5000):
        """Connect to coordinator"""
        try:
            self.reader, self.writer = await asyncio.open_connection(host, port)
            self.running = True
            
            print(f"\n{'='*60}")
            print(f"Worker {self.worker_id} started")
            print(f"  Coordinator: {host}:{port}")
            print(f"  AFS Server: {self.afs_client.server_addr}")
            print(f"  Cache: {self.afs_client.cache_dir}")
            print(f"{'='*60}\n")
            
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
            print(f"Registered with coordinator as {self.worker_id}")
        elif msg_type == 'task_assignment':
            await self.handle_task_assignment(message)
        elif msg_type == 'heartbeat_ack':
            pass  # Heartbeat acknowledged
        elif msg_type == 'snapshot_marker':
            await self.handle_snapshot_marker(message)
        elif msg_type == 'status':
            print(f"Status update: {message}")

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
        
        print(f"\n{'='*60}")
        print(f"Task {task_id} assigned: {filename}")
        print(f"{'='*60}")
        
        # Process file asynchronously
        asyncio.create_task(self.process_file(task_id, filename))

    async def handle_snapshot_marker(self, message):
        """Handle snapshot marker (Chandy-Lamport)"""
        snapshot_id = message.get('snapshot_id')
        
        print(f"Snapshot marker received: {snapshot_id}")
        
        # Save local state
        state = {
            'worker_id': self.worker_id,
            'tasks_processed': self.tasks_processed,
            'tasks_failed': self.tasks_failed,
            'cache_files': list(self.afs_client.cache.keys()),
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
        Process a file to find prime numbers
        Uses AFS client with caching
        """
        try:
            start_time = time.time()
            
            # Open file via AFS (with caching)
            print(f"[Task {task_id}] Opening {filename} via AFS...")
            content_bytes = await self.afs_client.open(filename)
            
            # Decode and parse
            content = content_bytes.decode('utf-8')
            
            if not content.strip():
                raise Exception(f"File is empty: {filename}")
            
            # Parse numbers
            numbers = []
            for line in content.split('\n'):
                line = line.strip()
                if line:
                    try:
                        num = int(line)
                        numbers.append(num)
                    except ValueError:
                        pass
            
            print(f"[Task {task_id}] Processing {len(numbers)} numbers...")
            
            # Find primes using Miller-Rabin
            primes = []
            for num in numbers:
                if self._is_prime(num):
                    primes.append(num)
            
            elapsed = time.time() - start_time
            
            # Send results to coordinator
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
                'result': f'Found {len(primes)} primes'
            })
            
            print(f"[Task {task_id}] ✓ Complete: {len(primes)}/{len(numbers)} primes found in {elapsed:.2f}s")
            print(f"{'='*60}\n")
            
            # Close file (flushes if modified)
            await self.afs_client.close(filename)
            
        except Exception as e:
            print(f"[Task {task_id}] ✗ Failed: {e}")
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


async def main():
    """Main entry point"""
    if len(sys.argv) > 1:
        worker_id = sys.argv[1]
    else:
        worker_id = f"worker-{random.randint(1000, 9999)}"

    # AFS server address
    afs_server = sys.argv[2] if len(sys.argv) > 2 else "localhost:8080"
    
    # Miller-Rabin iterations
    k = int(sys.argv[3]) if len(sys.argv) > 3 else 5

    worker = WorkerClient(worker_id, afs_server, k)

    try:
        await worker.connect()
        
        # Keep running
        while worker.running:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print(f"\n{worker_id} shutting down...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await worker.disconnect()
        print(f"{worker_id} stopped")


if __name__ == "__main__":
    asyncio.run(main())