import os

import yaml

from .. import app
from .error import AppError
from .mysql import init_db


class QueryItem():
    query_path = '{dir}/{file}'.format(
        dir=app.basedir,
        file=app.config['QUERY_PATH'])
    attrs = ('title', 'route', 'author',
             'access', 'source', 'columns', 'code', 'count')

    def __init__(self, name):
        for attr in self.attrs:
            setattr(self, attr, None)

        if os.path.exists(self.query_path):
            with open(self.query_path, encoding='utf-8') as f:
                yaml_data = yaml.load(f)

            for y_item in yaml_data:
                if y_item['route'] == name:
                    for attr in self.attrs:
                        setattr(self, attr, y_item.get(attr, None))
                    break
        else:
            raise AppError('FILE_NOT_FOUND', file=self.query_path)

    def get_result(self, page_size=0, current_page=1):
        if self.source == 'iqg_ro':
            return self.get_mysql_result(page_size, current_page)

        raise AppError('UNKNOWN_SOURCE', source=self.source)

    def get_result_count(self):
        if self.source == 'iqg_ro':
            return self.get_mysql_result_count()

        raise AppError('UNKNOWN_SOURCE', source=self.source)

    def get_mysql_result(self, pagesize, current_page):
        data = None
        cnx = init_db(self.source)
        with cnx.cursor() as cursor:
            sql = self.code
            if pagesize != 0:
                start_recorder = (current_page - 1) * pagesize
                end_recorder = current_page * pagesize
                sql += ' limit {start}, {end}'.format(start=start_recorder,
                                                      end=end_recorder)
            cursor.execute(sql)
            data = cursor.fetchall()
            if not self.columns:
                self.columns = []
                for col in cursor.description:
                    print(col)
                    self.columns.append(col[0])
        cnx.close()

        return data

    def get_mysql_result_count(self):
        cnx = init_db(self.source)
        cnt = 0
        with cnx.cursor() as cursor:
            cursor.execute(self.count)
            cnt = cursor.fetchone()[0]

        cnx.close()
        return cnt
