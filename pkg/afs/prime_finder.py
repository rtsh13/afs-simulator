import random

class PrimeFinder:
    def __init__(self, k=5):
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

    def _ferma_primality_test(self, n):
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
            if self._power(a, n-1, n) != 1:
                return False
        return True
    
    def is_prime(self, n):
        return self._ferma_primality_test(n)