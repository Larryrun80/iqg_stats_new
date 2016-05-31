import os

import yaml

from .. import app
from .stats_base import StatsBase


class QueryItem(StatsBase):
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
            raise self.AppError('FILE_NOT_FOUND', file=self.query_path)

    def get_result(self, page_size=0, current_page=1):
        if self.source == 'iqg_ro':
            data = self.get_mysql_result(self.source,
                                         self.code,
                                         page_size,
                                         current_page)
            if not self.columns:
                self.columns = data['columns']
            return data['data']

        raise self.AppError('UNKNOWN_SOURCE', source=self.source)

    def get_result_count(self):
        if self.source == 'iqg_ro':
            return self.get_mysql_result_count(self.source, self.count)

        raise self.AppError('UNKNOWN_SOURCE', source=self.source)
