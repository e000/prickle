from twisted.internet.defer import Deferred, DeferredSemaphore, DeferredLock, maybeDeferred
from functools import wraps

class DeferredConcurrencyLimiter:
    """Initiliaze me, and then use me as a decorator, to limit the ammount of defers that can execute asynchronously."""
    
    def __init__(self, tokens = 5):
        if tokens < 1:
            raise ValueError("tokens must be > 0")
        
        if tokens == 1:
            self.lock = DeferredLock()
        else:
            self.lock = DeferredSemaphore(tokens)
    
    def _releaseLock(self, response, lock):
        lock.release()
        return response
    
    def _lockAcquired(self, lock, f, *a, **kw):
        d = maybeDeferred(f, *a, **kw)
        d.addBoth(self._releaseLock, lock)
        return d
    
    def __call__(self, f):
        @wraps(f)
        def wrapped(*a, **kw):
            d = self.lock.acquire()
            d.addCallback(self._lockAcquired, f, *a, **kw)
            return d
        
        return wrapped

from functools import partial as _wrapFn

def deferToProcessPool(pool, func, *a, **kw):
    
    d = Deferred()
    def _deferToPoolCallback(result):
        if isinstance(result, Exception):
            print result
            d.errback(result)
        else:
            d.callback(result)
    a = (func,) + a
    
    pool.apply_async(_wrapFn, a, kw, _deferToPoolCallback)
    
    return d
    