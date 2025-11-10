import asyncio
import json
import sys
import random
import time
import os
import base64
import hashlib


class AFSClient:
    def __init__(self, clientID, cacheDir, replicaAddrs, maxRetries=3, retryDelay=1):
        self.client_id = clientID
        self.cache_dir = cacheDir
        self.replica_addrs = replicaAddrs
        self.max_retries = maxRetries
        self.retry_delay = retryDelay
        
        self.cache = {}
        os.makedirs(cacheDir, exist_ok=True)
        
        print(f"Client {clientID} initialized")
        print(f"Servers: {', '.join(self.replica_addrs)}")
        print(f"Cache dir: {cacheDir}")

    async def _rpc_call(self, method_name: str, params: dict):
        """Make JSON-RPC call to Go server with failover and retry logic (Task 2 & 3)."""

        for server_addr in self.replica_addrs:
            try:
                host, port_str = server_addr.split(':')
                port = int(port_str)
            except ValueError:
                print(f"Skipping invalid replica address: {server_addr}")
                continue 
            
            for attempt in range(self.max_retries):
                try:
                    reader, writer = await asyncio.open_connection(host, port)
                    
                    request = {
                        "method": method_name,
                        "params": [params],
                        "id": random.randint(1, 100000)
                    }

                    json_request = json.dumps(request)
                    writer.write(json_request.encode('utf-8'))
                    await writer.drain()

                    data = await reader.read(10 * 1024 * 1024)
                    writer.close()
                    await writer.wait_closed()
                    
                    if not data:
                        raise Exception("Empty response from server")

                    response = json.loads(data.decode('utf-8'))
                    
                    if response.get("error"):
                        raise Exception(f"RPC Error: {response['error'].get('message', 'Unknown')}")
                    
                    result = response.get("result")
                    
                    if result is None:
                        # This happens if Go RPC returns a nil or empty response, but no error.
                        if method_name in ("FileServer.StoreFile", "FileServer.CreateFile"):
                            raise Exception(f"NON_NETWORK_FATAL: Empty result for write operation on {server_addr}")
                        return {} # For other successful operations with no result body

                    if method_name in ("FileServer.StoreFile", "FileServer.CreateFile") and not result.get('Success', True) and result.get('Error') == "not primary server":
                        print(f"[{method_name}] Replica {server_addr} is a backup. Failing over to next replica...")
                        raise Exception("IS_BACKUP")
                    
                    if not result.get('Success', True):
                        raise Exception(result.get('Error', 'Operation failed'))

                    return result 
                    
                except (ConnectionRefusedError, ConnectionResetError, TimeoutError, OSError) as e:
                    if attempt < self.max_retries - 1:
                        print(f"[{method_name}] Connection failed on {server_addr} (Attempt {attempt + 1}/{self.max_retries}): {e}. Retrying in {self.retry_delay}s...")
                        await asyncio.sleep(self.retry_delay)
                        continue 
                    else:
                        print(f"[{method_name}] All {self.max_retries} attempts failed on {server_addr}. Trying next replica...")
                        break

                except Exception as e:
                    if str(e) == "IS_BACKUP":
                        break 
                    
                    print(f"[{method_name}] Fatal RPC error on {server_addr}: {e}. Trying next replica...")
                    break
            
        raise Exception("System Unreachable: All replicas failed.")
    
    def _get_cache_path(self, filename):
        return os.path.join(self.cache_dir, filename)

    async def open(self, filename):
        print(f"[AFS] Opening {filename}...")
        
        cache_path = self._get_cache_path(filename)
        
        # Check if file is in cache
        if filename in self.cache:
            print(f"[AFS] File {filename} found in cache, validating...")
            
            # what if someone deleted/modified the file on server end?
            # does it make sense to use the cached file on client end? no
            # we are verifying this via TestAuth
            cachedVno = self.cache[filename]['version']
            
            try:
                resp = await self._rpc_call("ReplicaServer.TestAuth", 
                    {"ClientID": self.client_id,"Filename": filename,"Version": cachedVno})
                
                if resp.get('Valid'):
                    print(f"[AFS] Cache valid for {filename} (version {cachedVno})")
                    with open(cache_path, 'rb') as f:
                        content = f.read()
                    return content
                
                # the cache got stale
                else:
                    print(f"[AFS] Cache stale for {filename}, fetching fresh copy")

            # in all other conditions, always try to fetch new copy
            except Exception as e:
                print(f"[AFS] TestAuth failed: {e}, fetching fresh copy")
        
        print(f"[AFS] Fetching {filename} from server...")
        content = await self._fetch_from_server(filename)
        
        return content

    async def _fetch_from_server(self, filename):
        result = await self._rpc_call("ReplicaServer.FetchFile", 
                {"ClientID": self.client_id,"Filename": filename})
        
        # server gives back []bytes
        # hence we decode it
        content_bytes = base64.b64decode(result['Content'])
        version = result['Version']
        
        # storing the buffer ie /tmp folder
        cache_path = self._get_cache_path(filename)
        with open(cache_path, 'wb') as f:
            f.write(content_bytes)
        
        # update cache metadata
        self.cache[filename] = {'version': version,'path': cache_path,'dirty': False,'size': len(content_bytes)}
        
        print(f"[AFS] Cached {filename} (version {version}, {len(content_bytes)} bytes)")
        
        return content_bytes

    # write to cache and mark is dirty
    # it will be flused
    async def write(self, filename, content):
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

    # flush to server
    async def close(self, filename):
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
        cache_path = self._get_cache_path(filename)
        
        # Read from local cache
        with open(cache_path, 'rb') as f:
            content = f.read()
        
        store_req = {
            "ClientID": self.client_id,
            "Filename": filename,
            "Content": base64.b64encode(content).decode('utf-8')  # Encode to base64
        }
        
        result = await self._rpc_call("ReplicaServer.StoreFile", store_req)
        
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
        
        await self._rpc_call("ReplicaServer.CreateFile", create_req)
        print(f"[AFS] Created file {filename} on server")

    def clear_cache(self):
        """Clear all cached files"""
        for filename in list(self.cache.keys()):
            cache_path = self._get_cache_path(filename)
            if os.path.exists(cache_path):
                os.remove(cache_path)
        self.cache.clear()
        print(f"[AFS] Cache cleared")


# worker finds prime numbers
# it is also repsonsible for implementing the snapshots
class WorkerClient(object):
    def __init__(self, workerID, serverAddrs, k=5):
        self.workerID = workerID
        self.reader = None # this is referring to StreamReader
        self.writer = None # this is referring to StreamWriter
        self.running = False
        self.buffer = b"" # empty byte string
        self.tasks_processed = 0
        self.tasks_failed = 0
        self.k = k  # Miller-Rabin iterations
        
        # requirement for caching files on disk
        # won't be stored in the cwd
        cacheDir = f"/tmp/afs-{workerID}"
        self.client = AFSClient(workerID, cacheDir, serverAddrs)
        
        # Snapshot state
        self.snapshotState = {}

    def _power(self, a, n, p):
        result = 1
        a = a % p
        while n > 0:
            if n % 2 == 1:
                result = (result * a) % p
            n = n >> 1
            a = (a * a) % p
        return result

    def _miller_rabin(self, n):
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
        return self._miller_rabin(n)

    # connect to the running coordinator
    async def connect(self, host='localhost', port=5000):
        try:
            self.reader, self.writer = await asyncio.open_connection(host, port)
            self.running = True
            
            print(f"\n{'='*50}")
            print(f"Worker {self.workerID} started")
            print(f"Coordinator: {host}:{port}")
            print(f"AFS Servers: {', '.join(self.client.replica_addrs)}") 
            print(f"Cache: {self.client.cache_dir}")
            print(f"{'='*50}\n")
            
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
        while self.running:
            try:
                # pull just 4KB worth of data from network pipe
                # you can pull more, i just wrote 4KB for now
                data = await self.reader.read(4096)
                if not data:
                    print("Connection closed by coordinator")
                    self.running = False
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
        
        if msgType == 'heartbeat_ack':
            pass
        elif msgType == 'request_id':
            await self.requestIDHandler(message)
        elif msgType == 'task_assignment':
            await self.taskAssignmentHandler(message)
        elif msgType == 'snapshot_marker':
            await self.spanshotMarkerHandler(message)
        elif msgType == 'status':
            print(f"Status update: {message}")
        elif msgType == 'registered':
            print(f"registered with coordinator as {self.workerID}")

    # respond to coordinator's request to send the worker id
    async def requestIDHandler(self, message):
        await self.send_message({'type': 'register','worker_id': self.workerID})

    async def taskAssignmentHandler(self, message):
        task_id = message.get('task_id')
        filename = message.get('filename')
        
        print(f"\n{'='*60}")
        print(f"Task {task_id} assigned: {filename}")
        print(f"{'='*60}")
        
        # Process file asynchronously
        asyncio.create_task(self.process_file(task_id, filename))

    # handle the broadcast marker request and respond back the state of the worker
    async def spanshotMarkerHandler(self, message):
        snapshot_id = message.get('snapshot_id')
        
        print(f"Snapshot marker received: {snapshot_id}")
        
        # Save local state
        state = {
            'worker_id': self.workerID,
            'tasks_processed': self.tasks_processed,
            'tasks_failed': self.tasks_failed,
            'cache_files': list(self.client.cache.keys()),
            'timestamp': time.time()
        }
        
        # Send state to coordinator
        await self.send_message({
            'type': 'snapshot_state',
            'snapshot_id': snapshot_id,
            'worker_id': self.workerID,
            'state': state
        })

    async def process_file(self, task_id, filename):
        try:
            start_time = time.time()
            
            # Open file via AFS (with caching)
            print(f"[Task {task_id}] Opening {filename} via AFS..")
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
                'worker_id': self.workerID
            })
            
            self.tasks_processed += 1
            
            # Mark task complete
            await self.send_message({
                'type': 'task_complete',
                'task_id': task_id,
                'result': f'Found {len(primes)} primes'
            })
            
            print(f"[Task {task_id}] Complete: {len(primes)}/{len(numbers)} primes found in {elapsed:.2f}s")
            print(f"{'='*60}\n")
            
            # Close file (flushes if modified)
            await self.client.close(filename)
            
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
        workerID = f"worker-{random.randint(1000, 9999)}"

    # pull the replica addresses
    if len(sys.argv) > 2:
        serversAddrs = sys.argv[2].split(',')
    else:
        serversAddrs = ["localhost:8080"]
    
    #pull the milner's number
    k = int(sys.argv[3]) if len(sys.argv) > 3 else 5 

    worker = WorkerClient(workerID, serversAddrs, k)

    try:
        await worker.connect()
        while worker.running:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print(f"\n{workerID} shutting down.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await worker.disconnect()
        print(f"{workerID} stopped")

if __name__ == "__main__":
    asyncio.run(main())