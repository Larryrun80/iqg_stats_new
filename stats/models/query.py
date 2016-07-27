import os

import yaml

from .. import app
from .stats_base import StatsBase


class QueryItem(StatsBase):
    query_path = '{dir}/{file}'.format(
        dir=app.basedir,
        file=app.config['QUERY_PATH'])

    def __init__(self, name):
        self.attrs += ['source', 'code', 'count']
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

    def get_result(self, page_size=0, current_page=1, sort=None):
        if self.source in ('iqg_ro', 'hsq_ro'):
            exec_code = self.code
            if sort:
                exec_code = 'select * from ({sql})t order by "{sort}"'.format(
                    sql=self.code,
                    sort=sort)

            data = self.get_mysql_result(self.source,
                                         exec_code,
                                         page_size,
                                         current_page)

            self.columns = data['columns']
            return data['data']

        raise self.AppError('UNKNOWN_SOURCE', source=self.source)

    def get_result_count(self):
        if self.count and self.source in ('iqg_ro', 'hsq_ro'):
            return self.get_mysql_result_count(self.source, self.count)

        return None
