import os

import yaml

from .. import app
from .stats_base import StatsBase


class QueryItem(StatsBase):
    query_path = '{dir}/{file}'.format(
        dir=app.basedir,
        file=app.config['QUERY_PATH'])

    def __init__(self, name):
        self.attrs += ['source', 'code', 'paging',
                       'sort_cols', 'filters', 'params', 'summary']
        for attr in self.attrs:
            setattr(self, attr, None)
        self.count = None  # for pagination

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
        exec_code = self.code
        if sort:
            exec_code = 'select * from ({sql})t order by {sort}'.format(
                sql=self.code,
                sort=sort)

        data = self.get_data(self.source,
                             exec_code,
                             page_size,
                             current_page)

        self.columns = data['columns']

        if self.summary:
            sl = []
            for i, op in enumerate(self.summary):
                value = '-'
                if op == '+':
                    value = 0
                    for row in data['data']:
                        if type(row[i]) in (int, float):
                            value += float(row[i])
                sl.append(value)

            data['data'] = list(data['data']) + [sl]
        return data['data']

    def get_result_count(self):
        return self.get_data_count(self.source, self.count)
