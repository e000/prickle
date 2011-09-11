from twisted.internet.task import LoopingCall
import rrdtool
import os.path
from twisted.python import log
import logging

class baseTemplate:
    """
        @var dataFactory
    """
    interval = 60
    gatherFirst = False # Do we want to use the first request to create baseline stats?
    dataFactory = None # The class that will hold our outputted data, to be put into our database!!
    requestsSent = 0
    successfulRequests = 0
    failedRequests = 0
    doingWork = False
    waitTillFinish = False
    numGraphs = 0
    
    def __init__(self, filename = None, testing = False, id = None, config = None, factory = None):
        assert filename or testing
        self.id = id
        self.filename = filename
        self.config = config
        self.factory = factory
        self._testing = testing
        self.loopingCall = LoopingCall(self._do_work)
        self.running = False
        self.init()

    def __repr__(self):
        return '<templates.%s id=%r, requestsSent=%i, successfulRequests=%i, failedRequests=%i, running=%r, doingWork=%r>' % ( 
            self.__class__.__name__.lower(), self.id, self.requestsSent, self.successfulRequests, self.failedRequests, self.running, self.doingWork
        )
        
    def __cmp__(self, other):
        return cmp(self.id, other.id)

    def do_work(self):
        raise NotImplemented()
        
    def _do_work(self):
        if self.doingWork:
            log.msg('%r tried to do work while busy!', logLevel = logging.ERROR)
            return
        self.doingWork = True
        d = self.do_work() \
         .addCallback(self.complete) \
         .addErrback(self.failed)
        if self.waitTillFinish:
            return d
        
    def update(self, data):
        raise NotImplemented()
        
    def _update(self, data):
        ret = rrdtool.update(
            self.filename, data
        )
        return ret

    def create(self):
        """Return a tuple that I will use to create a rrd database"""
        raise NotImplemented()

    def _create(self, overwrite = False):
        if os.path.exists(self.filename) and not overwrite:
            log.msg('Database %r already exists, not overwriting!' % self.filename, logLevel = logging.INFO)
            return
        
        fmt_dict = {
            'interval': self.interval,
            '2interval': self.interval*2,
        }
        
        rrdtool.create(self.filename, *[ln % fmt_dict for ln in self.create()])
        log.msg('Database %r created successfully!' % self.filename, logLevel = logging.INFO)
    
    def parse(self, data):
        """Parse the data and return an object provided by self.dataFactory"""
        raise NotImplemented()

    def _graph(self):
        for period in self.config['periods']:
            fmt_dict = {
                'period': period,
                'filename': self.filename,
                'id': self.id
            }
            for i, graph in enumerate(self.graph()):
                filename = os.path.join(self.factory.stats.config['image_path'], '%s-%s.%i.png' % (self.id, period, i))
                log.msg('Generating graph %r!' % filename, logLevel = logging.DEBUG)
                try:
                    rrdtool.graph(
                        filename,
                        *[ln % fmt_dict for ln in graph]
                    )
                except:
                    log.err()
        

    def run(self):
        assert not self.loopingCall.running
        assert not self.running
        self.running = True
        if self.interval > 0:
            self.loopingCall.start(self.interval)
        
    def stop(self):
        if self.running:
            self.running = False
            if self.loopingCall.running:
                self.loopingCall.start()
        
    def failed(self, err):
        self.doingWork = False
        self.failedRequests += 1
        log.msg("%r has encountered an error: " % self, logLevel = logging.ERROR)
        log.err(err)

    def complete(self, val):
        self.doingWork = False
        self.successfulRequests += 1
        
        log.msg('%r completed a successful work cycle' % self, logLevel = logging.DEBUG)