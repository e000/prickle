"""
    prickle.templates.mysql
    ~~~~~~~~~~~~~~~~~~~~~~~
    
    I combine the data from multiple `prickle.templates.nginx` into one nice graph.
    
    :copyright: (c) 2001 Edgeworth E. Euler
    :license: BSD!
"""

from stats.base import BaseTemplate
from twisted.internet import reactor, protocol
from twisted.internet.threads import deferToThread
from twisted.python import log
import logging
try:
    import MySQLdb as mysqldb
except ImportError:
    try:
        import pymysql as mysqldb
    except ImportError:
        raise RuntimeError("No appropriate mysql drivers found :(")


DEFAULT_PORT = 3306

class MySQL(BaseTemplate):
    interval = 60
    numGraphs = 4
    aliases = ['queries', 'qcache', 'handler', 'io']
    
    def init(self):
        self.config.setdefault('port', DEFAULT_PORT)
        self.config.setdefault('idle_timeout', self.interval + 10)
        self.config.setdefault('connect_timeout', 15)
        
    def _threadedWork(self):
        wanted = self._wantedFieldsSet
        conn = mysqldb.connect(
            host = self.config['host'],
            passwd = self.config['passwd'],
            port = self.config['port'],
            user = self.config['user']
        )
        
        result = {}
        
        cur = conn.cursor()
        cur.execute('SHOW GLOBAL STATUS')
        for k, v in cur:
            k = k.lower()[:19]
            if k in wanted or k == 'uptime':
                result[k] = long(v)
                
        cur.close()
        conn.close()
        
        return result
        
    def stop(self):
        """ Override to disconnect from MySQL """
        self.conn.disconnect()
        BaseTemplate.stop(self)
    
    _wantedFields = [s.lower()[:19] for s in sorted([
        'Questions', 'Open_tables', 'Bytes_sent', 'Bytes_received', 'Open_files', 'Key_read_requests', 'Key_reads', 'Qcache_hits', 'Qcache_queries_in_cache', 'Qcache_not_cached',
        'Handler_read_key',
        'Handler_delete',
        'Handler_commit',
        'Handler_read_next',
        'Handler_read_prev',
        'Handler_read_rnd',
        'Handler_read_rnd_next',
        'Handler_write',
        'Handler_update',
        'Handler_read_first',
        'Handler_rollback',
        
    ])]
    _wantedFieldsSet = frozenset(_wantedFields)
    
    _gaugeFields = frozenset(s.lower()[:19] for s in [
        'Open_tables', 'Open_files', 'Qcache_queries_in_cache'
    ])
    
    #dataFactory = lambda self, data: dict((k, int(v)) for k, v in data.iteritems() if k in self._wantedFieldsSet)
    
    def create(self):
        yield '-s %(interval)i'
        for field in self._wantedFields:
            yield "DS:%s:%s:%%(2interval)s:0:U" % (
                field, 'GAUGE' if field in self._gaugeFields else 'ABSOLUTE',
            )
        
        yield  "RRA:AVERAGE:0.5:1:2880"
        yield  "RRA:AVERAGE:0.5:30:672"
        yield  "RRA:AVERAGE:0.5:120:732"
        yield  "RRA:AVERAGE:0.5:720:1460"
        
    
    _handlerFields = (
        ('#FF0000', 'commit'),
        ('#CC00FF', 'read_first'),
        ('#3200FF', 'read_key'),
        ('#0065FF', 'read_next'),
        ('#00FF65', 'read_rnd'),
        ('#33FF00', 'read_rnd_next'),
        ('#CBFF00', 'update'),
        ('#FF9800', 'write'),
        ('#000000', 'rollback')
        
    )
    
    def _generateHandlerGraph(self):
        yield "-s -1%(period)s"
        yield "-t %(id)s handler stats"
        yield "--lazy"
        yield "-h300"
        yield "-w700"
        yield "-l0"
        yield "-aPNG"
        yield "-v invocations/sec"
        for _, key in self._handlerFields:
            yield "DEF:%s=%%(filename)s:%s:AVERAGE" % (key, ('handler_'+key)[:19])
            
        for i, (color, field) in enumerate(self._handlerFields):
            yield "AREA:%s%s:%-15s%s" % (field, color, field, ':STACK' if i > 0 else '')
            yield "GPRINT:"+field+":MAX:  Max\\: %%8.1lf %%S"
            yield "GPRINT:"+field+":AVERAGE: Avg\\: %%8.1lf %%S"
            yield "GPRINT:"+field+":LAST: Current\\: %%8.1lf %%S\\r"

        yield "HRULE:0#000000"
    
    def graph(self):
        return [
            (
                "-s -1%(period)s",
                "-t %(id)s queries/second",
                "--lazy",
                "-h", "150", "-w", "700",
                "-l 0",
                "-a", "PNG",
                "-v queries/sec",
                "DEF:questions=%(filename)s:questions:AVERAGE",
                "AREA:questions#BFFF00:Queries",
                "GPRINT:questions:MAX:  Max\\: %%7.1lf %%S",
                "GPRINT:questions:AVERAGE: Avg\\: %%7.1lf %%S",
                "GPRINT:questions:LAST: Current\\: %%7.1lf %%S queries/sec\\r",
                "HRULE:0#000000"
            ),
            (
                "-s -1%(period)s",
                "-t %(id)s query cache hit/misses",
                "--lazy",
                "-h", "150", "-w", "700",
                "-l 0",
                "-a", "PNG",
                "-v requests/sec",
                "DEF:get_hits=%(filename)s:qcache_hits:AVERAGE",
                "DEF:get_missesp=%(filename)s:qcache_not_cached:AVERAGE",
                "CDEF:get_misses=get_missesp,-1,*",
                "AREA:get_hits#BFFF00:Cache Hits",
                "GPRINT:get_hits:MAX:  Max\\: %%7.1lf %%S",
                "GPRINT:get_hits:AVERAGE: Avg\\: %%7.1lf %%S",
                "GPRINT:get_hits:LAST: Current\\: %%7.1lf %%S hits/sec\\r",
                "AREA:get_misses#FF0000:Cache Misses",
                "GPRINT:get_missesp:MAX:  Max\\: %%7.1lf %%S",
                "GPRINT:get_missesp:AVERAGE: Avg\\: %%7.1lf %%S",
                "GPRINT:get_missesp:LAST: Current\\: %%7.1lf %%S miss/sec\\r",
                "HRULE:0#000000"
            ),
            self._generateHandlerGraph(),
            (
                "-s -1%(period)s",
                "-t %(id)s network io",
                "--lazy",
                "-h", "150", "-w", "700",
                "-l 0",
                "-a", "PNG",
                "-v bytes/second",
                "DEF:bytes_received=%(filename)s:bytes_received:AVERAGE",
                "DEF:bytes_sent=%(filename)s:bytes_sent:AVERAGE",
                "CDEF:inbits=bytes_received,8,*",
                "CDEF:outbits=bytes_sent,8,*",
                "AREA:bytes_received#FF0000:Traffic In",
                "GPRINT:inbits:LAST:   Current\\: %%6.1lf %%Sbps",
                "GPRINT:inbits:MIN:  Min\\: %%6.1lf %%Sbps",
                "GPRINT:inbits:AVERAGE: Avg\\: %%6.1lf %%Sbps",
                "GPRINT:inbits:MAX:  Max\\: %%6.1lf %%Sbps\\r",
                "AREA:bytes_sent#00FF00:Traffic Out:STACK",
                "GPRINT:outbits:LAST:  Current\\: %%6.1lf %%Sbps",
                "GPRINT:outbits:MIN:  Min\\: %%6.1lf %%Sbps",
                "GPRINT:outbits:AVERAGE: Avg\\: %%6.1lf %%Sbps",
                "GPRINT:outbits:MAX:  Max\\: %%6.1lf %%Sbps\\r",
                "HRULE:0#000000"
            )
        ]

    
    def do_work(self):
        return deferToThread(
            self._threadedWork
        )
    
    def update(self, data):
        if self.successfulRequests == 0:
            self._lastData = data
            return
        
        if data['uptime'] < self._lastData['uptime']:
            self._lastData = data
            log.msg('Detected server restart, invalidating current stats...', logLevel = logging.INFO)
            return
        
        curData = {}
        invalid = False
        for key in self._wantedFields:
            if key in self._gaugeFields:
                curData[key] = data[key]
            else:
                d = data[key] - self._lastData[key]
                if d < 0:
                    log.msg('[%s] field [%s] returned negative delta, possible server restart?' % (self.id, key), logLevel = logging.INFO)
                    invalid = True
                    break
                curData[key] = d
                
        self._lastData = data
        
        if invalid:
            return


        return ('N%s' % (':%i' * len(self._wantedFields))) % tuple([
            curData[key] for key in self._wantedFields
        ])

template = MySQL