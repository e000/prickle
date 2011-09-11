"""
    prickle.templates.nginxcombined
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    I combine the data from multiple `prickle.templates.nginx` into one nice graph.
    
    :copyright: (c) 2001 Edgeworth E. Euler
    :license: BSD!
"""

import rrdtool
from stats.base import BaseTemplate
from twisted.python import log
import os.path
import logging

class Nginx(BaseTemplate):
    interval = 0
    numGraphs = 1
    useDatabase = False
        
    def _graph(self):
        for period in self.config['periods']:
            fmt_dict = {
                'period': period,
                'filename': self.filename
            }
            filename = os.path.join(self.factory.stats.config['image_path'], '%s-%s.0.png' % (self.id, period))
            log.msg('Generating graph %r!' % filename, logLevel = logging.DEBUG)
            try:
                rrdtool.graph(
                    filename,
                    *list(self.graph(fmt_dict))
                )
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
