import random

class PrimeFinder:
    def __init__(self, k=5):
        self.k = k

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
    
    def is_prime(self, n):
        return self._miller_rabin(n)