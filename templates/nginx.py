"""
    prickle.templates.nginx
    ~~~~~~~~~~~~~~~~~~~~~~~
    
    I fetch and create data from properly configured nginx servers.
    
    :copyright: (c) 2001 Edgeworth E. Euler
    :license: BSD!
"""


from collections import namedtuple
from stats.base import BaseTemplate
from twisted.web.client import getPage
import re

class Nginx(BaseTemplate):
    dataFactory = namedtuple('nginxStatsTuple', 'active requests reading writing waiting')
    interval = 60
    numGraphs = 2
    
    def init(self):
        self.config.setdefault('port', 80)
    
    def create(self):
        return (
            "-s %(interval)i",
            "DS:active:GAUGE:%(2interval)s:0:60000",
            "DS:requests:ABSOLUTE:%(2interval)s:0:100000000",
            "DS:reading:GAUGE:%(2interval)s:0:60000",
            "DS:writing:GAUGE:%(2interval)s:0:60000",
            "DS:waiting:GAUGE:%(2interval)s:0:60000",
            "RRA:AVERAGE:0.5:1:2880",
            "RRA:AVERAGE:0.5:30:672",
            "RRA:AVERAGE:0.5:120:732",
            "RRA:AVERAGE:0.5:720:1460"
        )
        
    def graph(self):
        return [
            (
                "-s -1%(period)s",
                "-t %(id)s requests/second",
                "--lazy",
                "-h", "150", "-w", "700",
                "-l 0",
                "-a", "PNG",
                "-v requests/sec",
                "DEF:requests=%(filename)s:requests:AVERAGE",
                "AREA:requests#336600:Requests",
                "GPRINT:requests:MAX:  Max\\: %%5.1lf %%S",
                "GPRINT:requests:AVERAGE: Avg\\: %%5.1lf %%S",
                "GPRINT:requests:LAST: Current\\: %%5.1lf %%Sreq/sec",
                "HRULE:0#000000"
            ),
            (
                "-s -1%(period)s",
                "-t %(id)s open connections",
                "--lazy",
                "-h", "150", "-w", "700",
                "-l 0",
                "-a", "PNG",
                "-v requests/sec",
                "DEF:active=%(filename)s:active:AVERAGE",
                "DEF:reading=%(filename)s:reading:AVERAGE",
                "DEF:writing=%(filename)s:writing:AVERAGE",
                "DEF:waiting=%(filename)s:waiting:AVERAGE",

                "LINE2:active#22FF22:Total",
                "GPRINT:active:LAST:   Current\\: %%5.1lf %%S",
                "GPRINT:active:MIN:  Min\\: %%5.1lf %%S",
                "GPRINT:active:AVERAGE: Avg\\: %%5.1lf %%S",
                "GPRINT:active:MAX:  Max\\: %%5.1lf %%S\\n",
                
                "LINE2:reading#0022FF:Reading",
                "GPRINT:reading:LAST: Current\\: %%5.1lf %%S",
                "GPRINT:reading:MIN:  Min\\: %%5.1lf %%S",
                "GPRINT:reading:AVERAGE: Avg\\: %%5.1lf %%S",
                "GPRINT:reading:MAX:  Max\\: %%5.1lf %%S\\n",
                
                "LINE2:writing#FF0000:Writing",
                "GPRINT:writing:LAST: Current\\: %%5.1lf %%S",
                "GPRINT:writing:MIN:  Min\\: %%5.1lf %%S",
                "GPRINT:writing:AVERAGE: Avg\\: %%5.1lf %%S",
                "GPRINT:writing:MAX:  Max\\: %%5.1lf %%S\\n",
                
                "LINE2:waiting#00AAAA:Waiting",
                "GPRINT:waiting:LAST: Current\\: %%5.1lf %%S",
                "GPRINT:waiting:MIN:  Min\\: %%5.1lf %%S",
                "GPRINT:waiting:AVERAGE: Avg\\: %%5.1lf %%S",
                "GPRINT:waiting:MAX:  Max\\: %%5.1lf %%S\\n",

                "HRULE:0#000000"
            )
        ]
    
    def do_work(self):
        return getPage(
            'http://%s:%i/app_status' % (self.config['host'], self.config['port'])
        )
    
    _parseRegexps = re.compile(
        "Active connections:\s+(\d+) \r?\n"
        "server accepts handled requests\r?\n"
        "\s+\d+\s+\d+\s+(\d+) \r?\n"
        "Reading:\s+(\d+).*Writing:\s+(\d+).*Waiting:\s+(\d+)"
    )
    
    def parse(self, data):
        return self.dataFactory(*[int(i) for i in self._parseRegexps.match(data).groups()])
    
    _lastRequestsNumber = 0
    
    def update(self, data):
        if self.successfulRequests == 0:
            self._lastRequestsNumber = data.requests
            return
        
        if data.requests < self._lastRequestsNumber:
            requests = data.requests
        else:
            
            requests = data.requests - self._lastRequestsNumber
        
        self._lastRequestsNumber = data.requests
        
        return 'N:%i:%i:%i:%i:%s' % (data.active, requests, data.reading, data.writing, data.waiting)
        
        

template = Nginx

import unittest

class TestParser(unittest.TestCase):
    def setUp(self):
        self.toParse = 'Active connections: 4 \nserver accepts handled requests\n 1112965 1112965 1112965 \nReading: 0 Writing: 4 Waiting: 0 \n'
        self.expectedResult = Nginx.dataFactory(active=4, requests=1112965, reading=0, writing=4, waiting=0)
        
    def test_parser(self):
        cls = Nginx(testing = True)
        self.assertEqual(cls.parse(self.toParse), self.expectedResult)

if __name__ == '__main__':
    unittest.main()