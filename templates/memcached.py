from stats.base import BaseTemplate
from twisted.internet import reactor, protocol
from twisted.protocols.memcache import MemCacheProtocol, DEFAULT_PORT
from twisted.python import log
import logging

class Memcached(BaseTemplate):
    interval = 60
    numGraphs = 4
    
    def init(self):
        self.config.setdefault('port', DEFAULT_PORT)
    
    _wantedFields = [
        'curr_connections', 'get_hits', 'get_misses', 'bytes', 'bytes_read', 'bytes_written'
    ]
    _wantedFieldsSet = frozenset(_wantedFields)
    
    _gaugeFields = frozenset([
        'curr_connections', 'bytes'
    ])
    
    dataFactory = lambda self, data: dict((k, int(v)) for k, v in data.iteritems() if k in self._wantedFieldsSet)
    
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
        
        
    def graph(self):
        return [
            (
                "-s -1%(period)s",
                "-t %(id)s hit/misses",
                "--lazy",
                "-h", "150", "-w", "700",
                "-l 0",
                "-a", "PNG",
                "-v requests/sec",
                "DEF:get_hits=%(filename)s:get_hits:AVERAGE",
                "DEF:get_missesp=%(filename)s:get_misses:AVERAGE",
                "CDEF:get_misses=get_missesp,-1,*",
                "AREA:get_misses#FF0000:Cache Misses",
                "GPRINT:get_missesp:MAX:  Max\\: %%7.1lf %%S",
                "GPRINT:get_missesp:AVERAGE: Avg\\: %%7.1lf %%S",
                "GPRINT:get_missesp:LAST: Current\\: %%7.1lf %%S miss/sec\\r",
                "AREA:get_hits#BFFF00:Cache Hits",
                "GPRINT:get_hits:MAX:  Max\\: %%7.1lf %%S",
                "GPRINT:get_hits:AVERAGE: Avg\\: %%7.1lf %%S",
                "GPRINT:get_hits:LAST: Current\\: %%7.1lf %%S hits/sec\\r",
                "HRULE:0#000000"
            ), (
                "-s -1%(period)s",
                "-t %(id)s memory usage",
                "--lazy",
                "-h", "150", "-w", "700",
                "-l 0",
                "-a", "PNG",
                "-v bytes",
                "DEF:bytes=%(filename)s:bytes:AVERAGE",
                "AREA:bytes#FFAABB:Memory Usage",
                "GPRINT:bytes:MAX:  Max\\: %%7.1lf %%Sb",
                "GPRINT:bytes:AVERAGE: Avg\\: %%7.1lf %%Sb",
                "GPRINT:bytes:LAST: Current\\: %%7.1lf %%Sb\\r",
                "HRULE:0#000000"
            ), (
                "-s -1%(period)s",
                "-t %(id)s open connections",
                "--lazy",
                "-h", "150", "-w", "700",
                "-l 0",
                "-a", "PNG",
                "-v open connections",
                "DEF:curr_connections=%(filename)s:curr_connections:AVERAGE",
                "LINE2:curr_connections#22FF22:Connections",
                "GPRINT:curr_connections:LAST:   Current\\: %%5.1lf %%S",
                "GPRINT:curr_connections:MIN:  Min\\: %%5.1lf %%S",
                "GPRINT:curr_connections:AVERAGE: Avg\\: %%5.1lf %%S",
                "GPRINT:curr_connections:MAX:  Max\\: %%5.1lf %%S\\r",
                "HRULE:0#000000"
            ),
            (
                "-s -1%(period)s",
                "-t %(id)s network io",
                "--lazy",
                "-h", "150", "-w", "700",
                "-l 0",
                "-a", "PNG",
                "-v bytes/second",
                "DEF:bytes_read=%(filename)s:bytes_read:AVERAGE",
                "DEF:bytes_written=%(filename)s:bytes_written:AVERAGE",
                "CDEF:inbits=bytes_read,8,*",
                "CDEF:outbits=bytes_written,8,*",
                "AREA:bytes_read#FF0000:Traffic In",
                "GPRINT:inbits:LAST:   Current\\: %%6.1lf %%Sbps",
                "GPRINT:inbits:MIN:  Min\\: %%6.1lf %%Sbps",
                "GPRINT:inbits:AVERAGE: Avg\\: %%6.1lf %%Sbps",
                "GPRINT:inbits:MAX:  Max\\: %%6.1lf %%Sbps\\r",
                "AREA:bytes_written#00FF00:Traffic Out:STACK",
                "GPRINT:outbits:LAST:  Current\\: %%6.1lf %%Sbps",
                "GPRINT:outbits:MIN:  Min\\: %%6.1lf %%Sbps",
                "GPRINT:outbits:AVERAGE: Avg\\: %%6.1lf %%Sbps",
                "GPRINT:outbits:MAX:  Max\\: %%6.1lf %%Sbps\\r",
                "HRULE:0#000000"
            )
        ]
    
    def do_work(self):
        d = protocol.ClientCreator(reactor, MemCacheProtocol).connectTCP(self.config['host'], self.config['port'])
        d.addCallback(
            lambda proto: proto.stats().addBoth(
                lambda res: (res, proto.transport.loseConnection())[0]
            )
        )
        return d
    
    def update(self, data):
        if self.successfulRequests == 0:
            self._lastData = data
            return
        
        curData = {}
        for key in self._wantedFields:
            if key in self._gaugeFields:
                curData[key] = data[key]
            else:
                d = data[key] - self._lastData[key]
                if d < 0:
                    log.msg('[%s] field [%s] returned negative delta, possible server restart?' % (self.id, key), logLevel = logging.INFO)
                    return
                curData[key] = d
        
        str = ('N%s' % (':%i' * len(self._wantedFields))) % tuple([
            curData[key] for key in self._wantedFields
        ])
        
        self._lastData = data

        return str

template = Memcached