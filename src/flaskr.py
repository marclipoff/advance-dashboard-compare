
from flask import jsonify
from flask import make_response
from flask import request
from flask import Flask
import datetime
import logging
import traceback
from . import main

app = Flask('advance_comparison')


@app.route('/', methods=['GET'])
def heartbeat():
    return jsonify({'status': 'up and beating', 'timestamp': datetime.datetime.now()}), 201

@app.route('/compare', methods=['POST'])
def compare():

    def validate_request():

        if request.args is None:
            status = 'failure'
            message = 'missing arguments to request'
            response = jsonify({'status': status, 'message': message})
            response.status_code = 400
        else:
            response = None

        return response

    logging.info(f"Got post request for {request.args}")

    if validate_request() is not None:
        return validate_request()

    #get parameters from payload
    db_password = request.args.get('dbpassword')
    enterprise_token = request.args.get('enterprisetoken')

    try:
        logging.debug(f"Running for {db_password} {enterprise_token}")
        resp = main.do(db_password, enterprise_token)

        status = 'success'
        message = 'success'
        response = jsonify({'status': status,
                            'message': message,
                            'comparison': resp})
        response.status_code = 200

    except Exception as e:
        #catch errors, and send errors to response payload
        tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
        tb_str = "".join(tb_str)
        status = 'failure'
        message = f'rebalancing failed. {tb_str}. check logs'
        response = jsonify({'status': status, 'message': message})
        response.status_code = 500
        logging.error(tb_str)

    return response

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)