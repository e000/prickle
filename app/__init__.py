from twisted.web import static, server, wsgi, resource
from twisted.internet import reactor
from flask import Flask, render_template, url_for

class Root(resource.Resource):
    """ A hackish way to allow us to put children onto a wsgi resource!!! """
    def __init__(self, wsgi_resource):
        resource.Resource.__init__(self)
        self.wsgi_resource = wsgi_resource

    def getChild(self, path, request):
        path0 = request.prepath.pop(0)
        request.postpath.insert(0, path0)
        return self.wsgi_resource

class WebApp:
    """ The core webapp that powers the front end of the stats server."""
    def __init__(self, stats):
        self.stats = stats
        self.app = None
    
    def _create_app(self):
        """ Here we create and return the wsgi application, scoping it to this little namespace """
        app = Flask(__name__)
        app.config.update(self.stats.config.get('flask_config', {}))
        
        stats = self.stats
        config = stats.config
        graphs = stats.active_graphs
        
        app.jinja_env.globals.update(dict(
            graph_url = lambda graph, period, id = 0: url_for('graph', filename = '%s-%s.%i.png' % (graph.id, period, id or 0))
            
        ))       
        
        @app.route('/graphs/<filename>')
        def graph(filename):
            """This function will be overwritten by a static.File resource. Leave it here so we can use url_for :)"""
            return 'You should never see this, this is overwritten by twisted...'
        
        @app.route('/')
        def index():
            return render_template('index.html', graphs = graphs)
    
        @app.route('/<template>')
        @app.route('/<template>/<period>')
        @app.route('/<template>/<period>/<int:id>')
        @app.route('/<template>/<int:id>')
        def filter(template, period='hour', id = 0):
            _graphs = (graph for graph in graphs.values() if graph.template == template and period in graph.config['periods'])
            print 'qq'
            return render_template('index.html', graphs = _graphs, period = period, id = id)
    
        
        return app
    
    def run(self):
        app = self.app = self._create_app()

        wsgi_resource = Root(wsgi.WSGIResource(reactor, self.stats.wsgi_threadpool, app))
        wsgi_resource.putChild('graphs', static.File(self.stats.config['image_path']))
        
        site = server.Site(wsgi_resource)
        reactor.listenTCP(
            self.stats.config['httpd_port'], site
        )
    
        
        