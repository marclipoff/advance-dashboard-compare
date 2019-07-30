from src import main
import logging

db_password = 'R3p0rtRunn3r8139'
enterprise_token = 'bd7582800821623d44d2f9a59d4607f5d1316e6f'

resp = main.do(db_password, enterprise_token)

logging.info(resp)