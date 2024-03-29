"""
    prickle.base.template
    ~~~~~~~~~~~~~~~~~~~~~
    
    The base template class is here.
    
    :copyright: (c) 2011 Edgeworth E. Euler
    :license: BSD!
"""

from twisted.internet.task import LoopingCall
import os.path
from twisted.python import log
import logging
from twisted.internet.defer import maybeDeferred, inlineCallbacks, DeferredList
from twisted.internet.threads import deferToThreadPool
from twisted.internet import reactor
import stats.proc_rrd as rrdtool
import time

class BaseTemplate:
    """
        I am the base template.
    """
    interval = 60
    gatherFirst = False # Do we want to use the first request to create baseline stats?
    dataFactory = None # The class that will hold our outputted data, to be put into our database!!
    requestsSent = 0
    successfulRequests = 0
    failedRequests = 0
    numGraphs = 0
    sortPriority = 0
    
    doingWork = False
    waitTillFinish = False
    useDatabase = True
    
    def __init__(self, filename = None, testing = False, id = None, config = None, factory = None):
        """ DO NOT OVERWRITE ME, USE init() instead!!! """
        assert filename or testing
        self.id = id
        self.filename = filename
        self.config = config
        self.factory = factory
        self._testing = testing
        self.loopingCall = LoopingCall(self._do_work)
        self.running = False
        self.init()
        
    def init(self):
        """
            Initialize this template here.
        """

    def __repr__(self):
        return '<templates.%s id=%r, requestsSent=%i, successfulRequests=%i, failedRequests=%i, running=%r, doingWork=%r>' % ( 
            self.__class__.__name__.lower(), self.id, self.requestsSent, self.successfulRequests, self.failedRequests, self.running, self.doingWork
        )
        
    def __cmp__(self, other):
        """
            Compares me with another template, to allow for sorting web-site
        """
        return cmp((self.sortPriority, self.id), (other.sortPriority, other.id))

    def do_work(self):
        """
            I get called when the template has to "do work", i.e. poll a server and save to rrd,
            I get called every `interval` seconds. If `interval` is 0, I will never be called.
        """
        
        raise NotImplemented()
        
    def _do_work(self):
        """
            Private do_work method, I am responsible for calling do_work.
        """
        if self.doingWork:
            # Do not allow us to do two units of work at once... if we're waiting on a result, just
            # let this cycle go. 
            log.msg('%r tried to do work while busy!', logLevel = logging.ERROR)
            return
        
        self.doingWork = True
        self.requestsSent += 1
        
        d = maybeDeferred(self.do_work) \
         .addCallback(self.parse) \
         .addCallback(self.update) \
         .addCallback(self._update) \
         .addCallback(self.complete) \
         .addErrback(self.failed)
        
        
        # If we want to wait to schedule the next iteration of do_work after the current iteration
        # else, we schedule right away. 
        if self.waitTillFinish:
            return d
        
    def update(self, data):
        """
            Overwrite me and return what should be put in the RRD database.
           
            For example: 
               return "N:15:100" 
        """
        raise NotImplemented()
        
    def _update(self, data):
        """
            Internal update call, takes data returned by update() and updates the database
            using the threadpool.
        """
        if not data:
            return None
        
        return rrdtool.update(self.filename, data)

    def create(self):
        """ Return an iterable of strings that shall be used to create the database """
        raise NotImplemented()

    @inlineCallbacks
    def _create(self, overwrite = False):
        """ Internal _create call. """
        if not self.useDatabase:
            return
        
        if os.path.exists(self.filename) and not overwrite:
            log.msg('Database %r already exists, not overwriting!' % self.filename, logLevel = logging.INFO)
            return
        
        # Variables that shall be substituted in data returned by create()
        fmt_dict = {
            'interval': self.interval,
            '2interval': self.interval*2,
        }
        
        res = yield rrdtool.create(self.filename, *[ln % fmt_dict for ln in self.create()])
        log.msg('Database %r created successfully!' % self.filename, logLevel = logging.INFO)
    
    def parse(self, data):
        """
            Parse the data and return an object that will be passed to update()
            Will attempt to use dataFactory to parse the data by default.
            Override if what you need to do cannot be done in dataFactory.
        """
        if self.dataFactory:
            return self.dataFactory(data)
        else:
            return data
        
    def graph(self):
        """
            Return an iterable of strings that will be passed to rrdtool.graph to create this templates graphs.
            I will be 
        """
        raise NotImplementedError()

    def _graph(self, period):
        """
            Internal call to _graph, I deal with the stuff that gets sent from graph(),
            I can be overridden if more custom graphing behavior is required.
            
            I am called every every `config.graph_draw_frequency[period]` seconds.
        """

        fmt_dict = {
            'period': period,
            'filename': self.filename,
            'id': self.id,
            'timestamp': int(time.time()),
            'template': self.template,
        }
        
        defers = []
        
        for i, graph in enumerate(self.graph()):
            filename = '%s-%s.%i.png' % (self.id, period, i)
            defers.append(
                rrdtool.graph(
                    os.path.join(self.factory.stats.config['image_path'], filename),
                    *[ln % fmt_dict for ln in graph]
                ).addErrback(
                    self._graphError, filename
                ).addCallback(
                    self._graphSuccess, filename
                )
            )
            
        return DeferredList(defers, consumeErrors = True)
     
    def _graphError(self, err, filename):
        log.msg("Failed to generate graph %r!" % graph)
        log.err(err)
    
    def _graphSuccess(self, res, filename):
        ct = time.time()
        log.msg('Generated graph %r!' % (filename), logLevel = logging.DEBUG)
        self.factory.stats.last_draw_timestamp[filename] = int(ct)

    def run(self):
        """
            Run me, and error if somehow I'm called while I am running!
        """
        assert not self.loopingCall.running
        assert not self.running
        self.running = True
        if self.interval > 0:
            self.loopingCall.start(self.interval)
        
    def stop(self):
        """ Stop me. """
        if self.running:
            self.running = False
            if self.loopingCall.running:
                self.loopingCall.start()
        
    def failed(self, err):
        """ Something along the lines from do_work to update failed. """
        self.doingWork = False
        self.failedRequests += 1
        log.msg("%r has encountered an error: " % self, logLevel = logging.ERROR)
        log.err(err)

    def complete(self, val):
        """ do_work completed a successful unit of work, and no errors reached us. """
        
        self.doingWork = False
        self.successfulRequests += 1
        
        log.msg('%r completed a successful work cycle' % self, logLevel = logging.DEBUG)