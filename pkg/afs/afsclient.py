import asyncio
import json
import random
import os
import base64


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
                        if method_name in ("ReplicaServer.StoreFile", "ReplicaServer.CreateFile"):
                            raise Exception(f"NON_NETWORK_FATAL: Empty result for write operation on {server_addr}")
                        return {}

                    if method_name in ("ReplicaServer.StoreFile", "ReplicaServer.CreateFile") and not result.get('Success', True) and result.get('Error') == "not primary server":
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
            "Content": base64.b64encode(content).decode('utf-8')
        }
        
        result = await self._rpc_call("ReplicaServer.StoreFile", store_req)
        
        new_version = result['NewVersion']
        
        # Update cache metadata
        self.cache[filename]['version'] = new_version
        self.cache[filename]['dirty'] = False
        
        print(f"[AFS] Flushed {filename} to server (new version: {new_version})")

    async def create(self, filename):
        create_req = {
            "ClientID": self.client_id,
            "Filename": filename
        }
        
        await self._rpc_call("ReplicaServer.CreateFile", create_req)
        print(f"[AFS] Created file {filename} on server")

    def clear_cache(self):
        for filename in list(self.cache.keys()):
            cache_path = self._get_cache_path(filename)
            if os.path.exists(cache_path):
                os.remove(cache_path)
        self.cache.clear()
        print(f"[AFS] Cache cleared")
