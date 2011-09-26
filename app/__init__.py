from twisted.web import static, server, wsgi, resource
from twisted.internet import reactor
from flask import Flask, render_template, url_for, abort, redirect, request

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
        from stats.templates import templates, templates_dict
        app = Flask(__name__)
        
        app.config.update(self.stats.config.get('flask_config', {})) # propegate the app's config to Flask
        
        # We use this to compress html output, to save BW.
        app.jinja_options['extensions'].append('stats.app.jinja2htmlcompress.HTMLCompress')
        app.create_jinja_environment()
        
        # Local variables.
        stats = self.stats
        config = stats.config
        graphs = stats.active_graphs
        
        def graph_url(graph, period, id = 0):
            """ Generates an image URL for the graph, and appends a param to it that should invalidate the cache upon graph update. """
            filename = '%s-%s.%i.png' % (graph.id, period, id or 0)
            return url_for('graph', filename = filename, _ = stats.last_draw_timestamp.get(filename, -1))
                               
        
        app.jinja_env.globals.update(dict(
            graph_url = graph_url
            
        ))       
        
        @app.route('/graphs/<filename>')
        def graph(filename):
            """This function will be overwritten by a static.File resource. Leave it here so we can use url_for :)"""
            return 'You should never see this, this is overwritten by twisted...'
        
        @app.route('/')
        def index():
            return render_template('index.html', templates = templates)
        
        
        @app.route('/route', methods = ['POST'])
        def route():
            try:
                template_name, alias = request.form['application'].split('/', 1)
                period = request.form['period']
                if not template_name in templates_dict:
                    abort(404)
                if not alias in templates_dict[template_name].aliases_reversed:
                    abort(404)
                
                return redirect(url_for('filter', template_name = template_name, period = period, id = alias))
                
            except:
                raise
    
        @app.route('/<template_name>')
        @app.route('/<template_name>/<id>/<period>')
        @app.route('/<template_name>/<id>')
        def filter(template_name, period='hour', id = 0):
            if not template_name in templates_dict:
                abort(404)
            template = templates_dict[template_name]
            if not id in template.aliases_reversed:
                abort(404)
            alias = id
            id = template.aliases_reversed[id]
            
            _graphs = (graph for graph in graphs.values() if graph.template == template_name and period in graph.config['periods'])
            
            return render_template('graph.html', graphs = _graphs, period = period, id = id, template=template, templates = templates, alias = alias)
    
        
        return app
    
    def run(self):
        app = self.app = self._create_app()

        wsgi_resource = Root(wsgi.WSGIResource(reactor, self.stats.wsgi_threadpool, app))
        wsgi_resource.putChild('graphs', static.File(self.stats.config['image_path']))
        
        site = server.Site(wsgi_resource)
        reactor.listenTCP(
            self.stats.config['httpd_port'], site
        )
    
        
        