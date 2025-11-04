import random
import json
import socket
import sys
import time

class WorkerPrimeGuesser:
    def __init__(self,worker_id, host ='localhost', port=5000, k= 5):
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.k = k
        self.sock = None

        #for snapshot, I need help!!!
        self.currrent_file = None
        self.primes_found=0
        self.numbers_tested=0

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self._send({'type': 'register', 'worker_id': self.worker_id})
        ack = self._receive()
        print(f"Worker {self.worker_id} connected to server at {self.host}:{self.port}")


    def _power(self, a, n, p):
        #(a^n) % p
        result = 1
        a = a % p
        while n> 0:
            if n%2 ==1:
                result = (result * a ) % p
            n = n >> 1
            a = (a * a) % p
        return result
    def _is_prime(self, n):
        #Fermat!
        if n <2:
            return False
        if n <=3:
            return True
        if n % 2 == 0:
            return False
        for _ in range(self.k):
            a = random.randint(2, n-2)
            if self._power(a, n-1,n) != 1:
                return False
        return True
    