class baseTemplate:
    """
        @var dataFactory
    """
    dataFactory = None
    
    def __init__(self, testing = False):
        self._testing = testing
    
    def parse(self, data):
        """Parse the data and return an object provided by self.dataFactory"""
        raise NotImplemented()
