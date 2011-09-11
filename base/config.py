"""
    stats.base.config
    ~~~~~
    We parse and hold configuration options for the entire stats application.
    
    :copyright: (c) 2001 Edgeworth E. Euler
    :license: BSD!
    
"""

import copy

class Config(dict):
    
    def __init__(self, defaults):
        dict.__init__(self, copy.deepcopy(defaults or {}))
    
    def load(self, filename, silent=False):
        """
            Attempt to load the configuration from a python file.
            THIS WILL EVALUATE THE CONTENTS OF THE FILE, MAKE SURE
            YOUR CONFIGURATION FILE IS PROPERLY SANITIZED AND SANE!!
        """
        
        tmp = {}
        try:
            execfile(filename, tmp)
        except IOError, e:
            if silent and e.errno in (errno.ENOENT, errno.EISDIR):
                return False
            e.strerror = 'Unable to load configuration file (%s)' % e.strerror
            raise
        self.update(tmp)
        return True
    