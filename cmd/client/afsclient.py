import ctypes
client = ctypes.cdll.LoadLibrary('.\\client.so')
open = client.Open
read = client.Read
write = client.Write
close = client.Close
create = client.Create

def open(filename):
    opened = bool(open(filename.encode('utf-8')))
    return opened

def read(filename, offset, size):
    buffer = bytes(read(filename.encode('utf-8'), ctypes.c_int(offset), ctypes.c_int(size)))
    return buffer

def write(filename, offset, data):
    size = len(data)
    written = bool(int(write(filename.encode('utf-8'), ctypes.c_void_p(data), ctypes.c_int(offset), ctypes.c_int(size))))
    return written

def close(filename):
    closed = bool(int(close(filename.encode('utf-8'))))
    return closed

def create(filename):
    filename = bool(int(create(filename.encode('utf-8'))))
