import json
import re

from .error import AppError
from .mysql import init_mysql
from .mongo import init_mongo
from .. import app


class StatsBase(object):
    AppError = AppError
    app_configs = app.config
    basedir = app.basedir
    attrs = ['title', 'route', 'author', 'access']

    def __init__(self, **params):
        super().__init__(**params)

    def __str__(self):
        kvs = []
        for attr in self.attrs:
            kvs.append('{}: {}'.format(attr, getattr(self, attr)))

        return ' | '.join(kvs)

    def get_mysql_result(self, source, sql, pagesize=0, current_page=1):
        '''Get mysql result
           Load data from mysql and return

        arguments:
        source -- a string corresponding to tag in config, string
        sql -- a str of sql statement to execute
        pagesize -- how many items should be showed in one page, int
                    0 for don't pagenate
        current_page -- current_page index, int
                        if pagesize=0, this param will be skipped
        '''
        data = None
        columns = []
        cnx = init_mysql(source)
        with cnx.cursor() as cursor:
            if pagesize != 0:
                start_recorder = (current_page - 1) * pagesize
                end_recorder = current_page * pagesize
                sql += ' limit {start}, {end}'.format(start=start_recorder,
                                                      end=end_recorder)
            cursor.execute(sql)
            data = cursor.fetchall()

            for col in cursor.description:
                columns.append(col[0])
        cnx.close()

        return {'data': data, 'columns': columns}

    def get_mysql_result_count(self, source, sql):
        '''Get mysql result count
           Load data from mysql and return a count

        arguments:
        source -- a string corresponding to tag in config, string
        sql -- a str of sql statement, should be something like
               select count(0) from ...
        '''
        cnx = init_mysql(source)
        cnt = 0
        with cnx.cursor() as cursor:
            cursor.execute(sql)
            cnt = cursor.fetchone()[0]

        cnx.close()
        return cnt

    def get_mongo_result_count(self, source, code):
        '''Get mongo result count
           Load data from mongo, using find clause and return a count

        arguments:
        source -- a string corresponding to tag in config, string
        code -- a mongo statement using 'find' clause, like:
                [database].[collection].find({...})
        '''

        cnx = init_mongo(source)
        codes = code.split('.')
        if len(codes) < 3:
            raise AppError('WRONG_CODE', code=code)
        collection = cnx[codes[0]][codes[1]]  # get mongo collection
        # get condition from code and jsonlize it
        # step 1. find condition words
        condition_pattern = r'find\({.*\}\)'
        # start positon of find clause
        condition = re.findall(condition_pattern, code)
        if condition:
            condition = condition[0][5:-1]
            condition = json.loads(condition)
            result = collection.find(condition).count()
        else:
            raise AppError('INVALID_QUERY', query=code)

        cnx.close()
        return result

    def get_mongo_result(self, source, code):
        '''Get mongo result
           Load data from mongo and return, using find clause

        arguments:
        source -- a string corresponding to tag in config, string
        code -- a mongo statement using 'find' clause, like:
                [database].[collection].find({...})
        '''

        cnx = init_mongo(source)
        codes = code.split('.')
        if len(codes) < 3:
            raise AppError('WRONG_CODE', code=code)
        collection = cnx[codes[0]][codes[1]]  # get mongo collection
        # get condition from code and jsonlize it
        # step 1. find condition words
        condition_pattern = r'find\({.*\}\)'
        # start positon of find clause
        condition = re.findall(condition_pattern, code)
        if condition:
            condition = condition[0][5:-1]
            condition = json.loads(condition)
            cursor = collection.find(condition)
            result = []
            for document in cursor:
                result.append(document)
        else:
            raise AppError('INVALID_QUERY', query=code)

        cnx.close()
        return result

    def get_data_count(self, source, code):
        if source in app.config['MYSQL_SOURCES']:
            return self.get_mysql_result_count(source, code)
        elif source in app.config['MONGO_SOURCES']:
            return self.get_mongo_result_count(source, code)
        else:
            raise AppError('DB_TAG_ERROR', tag=source)

    def get_data(self, source, code):
        print(app.config)
        if source in app.config['MYSQL_SOURCES']:
            return self.get_mysql_result(source, code)
        elif source in app.config['MONGO_SOURCES']:
            return self.get_mongo_result(source, code)
        else:
            raise AppError('DB_TAG_ERROR', tag=source)
