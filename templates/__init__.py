"""
    prickle.templates
    ~~~~~~~~~~~~~~~~~
    
    i load templates by template name, and hold the TemplateRunner
    
"""
import os.path
import imp
from twisted.internet.task import LoopingCall
from twisted.internet.threads import deferToThreadPool
from twisted.internet.defer import DeferredList
from twisted.python import log
from twisted.internet import reactor
import logging
import time

_templates = {}


def load_template(name):
    """ Attempts to load a template by name """
    if name in _templates:
        return _templates[name].template
        
    else:
        fp, pathname, description = imp.find_module(name, __path__)
        try:
            template = _templates[name] = imp.load_module(name, fp, pathname, description)
            template.template.template = name
            return template.template
        
        finally:
            fp.close()
            
def template_exists(name):
    """ Checks if a template exists """
    try:
        load_template(name)
        return True
    except ImportError, AttributeError:
        raise
        return False
        

class TemplateRunner(object):
    """ The template runner, I manage all of the templates """
    
    def __init__(self, stats):
        self.stats = stats
        self.loopingCall = LoopingCall(self.render_graphs)
    
    def create_databases(self, overwrite = False):
        for graph in self.stats.config['graphs']:
            cls = load_template(graph['template'])
            Cls = cls(self.make_filename(graph['id']), id = graph['id'], config = graph['config'], factory = self)
            Cls._create(overwrite)
        
    def make_filename(self, id):
        """ Convenience function that creates the filename / path for an rrd database """
        return os.path.join(self.stats.config['database_path'], '%s.rrd' % id)
        
    def run(self):
        """ Start each of the configured templates up. """
        for graph in self.stats.config['graphs']:
            cls = load_template(graph['template'])
            Cls = cls(self.make_filename(graph['id']), id = graph['id'], config = graph['config'], factory = self)
            self.stats.active_graphs[graph['id']] = Cls
            Cls.run()
            
        self.loopingCall.start(self.stats.config['graph_frequency'], True)
            
    def stop(self):
        """ Stop the template runner """
        
        if self.loopingCall.running:
            self.loopingCall.stop()
            
        for template in self.stats.active_graphs.itervalues():
            template.stop()
            
        self.active_graphs.clear()
        
    
    def render_graphs(self):
        """
            Render all the graphs that each template provides, work happens
            in multiple threads. Returns a deferred that fires when graphs have
            been successfully rendered.
        """
        def do_graph_render(callback):
            try:
                callback()
            except:
                log.msg("Error generating graphs!", logLevel = logging.ERROR)
                log.err()
            
        log.msg('Generating graphs...', logLevel = logging.INFO)
        t = time.time()
        def workDone(res):
            log.msg('Generated graphs in %.3f seconds.' % (time.time() - t), logLevel = logging.INFO)
            return res
        
        d = DeferredList([
            deferToThreadPool(reactor, self.stats.rrd_threadpool, do_graph_render, template._graph) for template in self.stats.active_graphs.itervalues()
        ], consumeErrors = True)
        
        d.addCallback(workDone)
        
        return d

            
    
    
__all__ = ['TemplateRunner', 'load_template', 'template_exists']