from pymongo import MongoClient

from .. import app
from .error import AppError


def init_mongo(tag=''):
    params = ('host', 'port', 'user', 'passwd', 'db')
    mongo_params = []

    if tag != '':
        tag = tag + '_'
    for param in params:
        mongo_params.append('{tag}{param}'.format(tag=tag, param=param))

    db_conf = {}
    for param in params:
        conf_param = '{}{}'.format(tag, param)
        if conf_param in app.config.keys():
            db_conf[param] = app.config[conf_param]
        elif conf_param.upper() in app.config.keys():
            db_conf[param] = app.config[conf_param.upper()]
        else:
            raise AppError('MISSING_PARAM', param=conf_param)
    if 'port' in db_conf.keys():
        db_conf['port'] = int(db_conf['port'])

    db_str = 'mongodb://{0}:{1}@{2}:{3}/{4}'.format(db_conf['user'],
                                                    db_conf['passwd'],
                                                    db_conf['host'],
                                                    db_conf['port'],
                                                    db_conf['db'])
    cnx = MongoClient(db_str)
    return cnx
