"""
    prickle.stats
    ~~~~~~~~~~~
    
    Here, we define the base stats class
    
    :copyright: (c) 2011 Edgeworth E. Euler
    :license: BSD!
"""

from werkzeug.datastructures import ImmutableDict
from .templates import load_template, template_exists, TemplateRunner
from .app import WebApp
from .base.config import Config
from twisted.internet import reactor
from twisted.python.threadpool import ThreadPool
from twisted.python import log
import logging

class Stats(object):
    """
        stats.Stats rrdtool templated poller, updater, and web frontend
    """
    
    # Our default configuration directives, if they don't
    # exist in the configuration file, they'll reflect what's here
    default_config = ImmutableDict(**dict(
        graphs = [],
        httpd_port = 8080,
        interface = '0.0.0.0',
        graph_draw_frequency = dict(
            hour = 60,
            day = 300,
            week = 300*3,
            default = 60
        )
    ))

    def __init__(self, instance_name):
        self.instance_name = instance_name
        self.config = Config(self.default_config)
        self.flask_app = WebApp(self)
        self.template_runner = TemplateRunner(self)
        self.active_graphs = dict()
        self.rrd_threadpool = ThreadPool(minthreads=2, maxthreads=2, name = 'rrd_threadpool')
        self.wsgi_threadpool = ThreadPool(minthreads = 1, maxthreads=5, name = 'wsgi_threadpool')
    
    def validate_config(self):
        """ Validates the loaded configuration. """
        c = self.config
        
        # Make sure that we have a database_path, and an image_path...
        assert 'database_path' in c
        assert 'image_path' in c
        # We should probably check if these paths exist and make them as well...
        
        # Set the default values.
        graph_draw_frequency = c['graph_draw_frequency']
        for period, interval in self.default_config['graph_draw_frequency'].iteritems():
            graph_draw_frequency.setdefault(period, interval)
        
        # A quick check to make sure that our port is an integer.
        c['httpd_port'] = int(c['httpd_port'])
        
        # Make sure that no duplicate IDs exist, and that the template exists as well.
        ids = set()
        for graph in c['graphs']:
            graph.setdefault('config', {})
            graph['config'].setdefault('periods', [])
            assert graph['id'] not in ids
            ids.add(graph['id'])
            assert(template_exists(graph['template']))
            
    def create_databases(self, overwrite = False):
        """ A convenience function to create all rrd databases. """
        self.validate_config()
        self.template_runner.create_databases(overwrite)
        
    def start_threadpool(self, pool):
        """ Schedules the start of a threadpool, and schedule the stop of it when the reactor shuts down. """
        if not pool.started:
            reactor.callWhenRunning(self._really_start_threadpool, pool)
            
    def _really_start_threadpool(self, pool):
        """ Starts the threadpool with out scheduleing it via the reactor. """
        if pool.started:
            return
        pool.start()
        reactor.addSystemEventTrigger('after', 'shutdown', pool.stop)
        log.msg('Started threadpool [%s, min=%i, max=%i]' % (pool.name, pool.min, pool.max), logLevel = logging.INFO)
        
    def run(self, **config_args):
        """
            Run the stats application, example below:
            
            >>> from stats import Stats
            >>> s = Stats(__name__)
            >>> s.config.load('config.py')
            >>> s.create_databases()
            >>> s.run()

        """
        # Load and validate the configuration.
        self.config.update(config_args)
        self.validate_config()
        
        # Schedule the start of the threadpools.
        self.start_threadpool(self.rrd_threadpool)
        self.start_threadpool(self.wsgi_threadpool)
        
        # Start the web server and the template runner.
        self.template_runner.run()
        self.flask_app.run()
        
        # Finally, start the twisted reactor.
        reactor.run()
    