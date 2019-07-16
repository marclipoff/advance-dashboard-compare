from gevent import monkey; monkey.patch_all()
from src.flaskr import app
import argparse
from gevent.pywsgi import WSGIServer
from gevent.pool import Pool


parser = argparse.ArgumentParser()
parser.add_argument('--port', dest='port', type=int, default=80, action='store')
parser.add_argument('--host', dest='host', type=str, default='0.0.0.0', action='store')
parser.add_argument('--debug', dest='debug', action='store_true')


args = parser.parse_args()

if __name__ == '__main__':

    if args.debug:
        app.run(debug=args.debug, port=args.port, host=args.host)
    else:

        pool = Pool(5)
        http_server = WSGIServer((args.host, args.port), app, spawn=pool)
        http_server.serve_forever()