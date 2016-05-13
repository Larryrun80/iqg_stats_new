import pymysql

from .. import app
from .error import AppError


def init_db():
    mysql_params = ('host', 'port', 'user', 'passwd', 'db', 'charset')
    db_conf = {}
    for param in mysql_params:
        if param in app.config.keys():
            db_conf[param] = app.config[param]
        elif param.upper() in app.config.keys():
            db_conf[param] = app.config[param.upper()]
        else:
            raise AppError('MISSING_PARAM', param=param)
    if 'port' in db_conf.keys():
        db_conf['port'] = int(db_conf['port'])
    cnx = pymysql.connect(**db_conf)
    return cnx
