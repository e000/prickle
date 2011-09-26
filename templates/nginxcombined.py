"""
    prickle.templates.nginxcombined
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    I combine the data from multiple `prickle.templates.nginx` into one nice graph.
    
    :copyright: (c) 2011 Edgeworth E. Euler
    :license: BSD!
"""

import stats.proc_rrd as rrdtool
from stats.base import BaseTemplate
from twisted.python import log
from twisted.internet.defer import inlineCallbacks
import os.path
import time
import logging

class Nginx(BaseTemplate):
    interval = 0
    numGraphs = 1
    useDatabase = False
    aliases = ['requests']
    
    @inlineCallbacks
    def _graph(self, period):
        fmt_dict = {
            'period': period,
            'filename': self.filename
        }
        filename = '%s-%s.0.png' % (self.id, period)
        
        log.msg('Generating graph %r!' % filename, logLevel = logging.DEBUG)
        try:
            yield rrdtool.graph(
                os.path.join(self.factory.stats.config['image_path'], filename),
                *list(self.graph(fmt_dict))
            )
            self.factory.stats.last_draw_timestamp[filename] = int(time.time())
        except:
            log.err()
                
    def graph(self, fmt_dict):
        db = self.factory.make_filename
        colors = '#FFA500 #FF7F50 #FF0000 #FF00FF'.split(' ')
        
        yield "-s -1%(period)s" % fmt_dict
        yield "-t"
        yield "Combined requests on [%s]" % ', '.join(self.config['ids'])
        yield "--lazy"
        yield "-h"
        yield "150"
        yield "-w"
        yield "700"
        yield "-l 0"
        yield "-a"
        yield "PNG"
        yield "-v requests/sec"
        
        for i, id in enumerate(self.config['ids']):
            yield "DEF:requests%i=%s:requests:AVERAGE" % (i, db(id))
        
        for i, (id, color) in enumerate(zip(self.config['ids'], colors)):
            yield 'AREA:requests%i%s:%s%s' % (
                i, color, id, '' if i == 0 else ':STACK'
            )
            yield "GPRINT:requests%i:MAX:  Max\\: %%5.1lf %%S" % i
            yield "GPRINT:requests%i:AVERAGE: Avg\\: %%5.1lf %%S" % i
            yield "GPRINT:requests%i:LAST: Current\\: %%5.1lf %%Sreq/sec\\r" % i
        
        yield "HRULE:0#000000"



template = Nginx
