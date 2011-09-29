from twisted.internet.utils import getProcessValue, getProcessOutput
from util import DeferredConcurrencyLimiter
from twisted.python.procutils import which

_limit = DeferredConcurrencyLimiter(10) # maximum ammounts of processes to run concurrently, 10 seems fine...

try:
    _rrdpath = which('rrdtool')[0]
except IndexError:
    raise RuntimeError("rrdtool could not be found in $PATH, is it installed?")
    
"""I've decided against using python's rrdtool, I think it leaks memory... anyways, this is a lot faster as well, as I don't need to defer to threads anymore!"""

@_limit
def create(filename, *params):
    args = ['create', filename] + list(params)
    return getProcessOutput(_rrdpath, args = args)
    

@_limit
def update(filename, *params):
    args = ['update', filename] + list(params)
    return getProcessValue(_rrdpath, args = args)

@_limit
def graph(filename, *params):
    args = ['graph', filename] + list(params)
    return getProcessValue(_rrdpath, args = args)
    
__all__ = ['create', 'update', 'graph']