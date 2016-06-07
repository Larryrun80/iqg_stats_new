import os

import yaml

from .. import app
from .stats_base import StatsBase


class FunnelItem(StatsBase):
    funnel_path = '{dir}/{file}'.format(
        dir=app.basedir,
        file=app.config['FUNNEL_PATH'])

    def __init__(self, name):
        self.attrs += ['conversion', 'base_ids', 'funnel']
        self.ids = []
        for attr in self.attrs:
            setattr(self, attr, None)

        if os.path.exists(self.funnel_path):
            with open(self.funnel_path, encoding='utf-8') as f:
                yaml_data = yaml.load(f)

            for y_item in yaml_data:
                if y_item['route'] == name:
                    for attr in self.attrs:
                        setattr(self, attr, y_item.get(attr, None))
                    break
        else:
            raise self.AppError('FILE_NOT_FOUND', file=self.funnel_path)

    def get_base_ids(self):
        if self.base_ids['source'] == 'iqg_ro':
            data = self.get_mysql_result(self.base_ids['source'],
                                         self.base_ids['ids'])
            for row in data['data']:
                self.ids.append(str(row[0]))

        if self.base_ids['source'] == 'list':
            if isinstance(self.base_ids['ids'], list):
                for uid in self.base_ids['ids']:
                    self.ids.append(str(uid))

        if not self.ids:
            raise self.AppError('UNKNOWN_SOURCE', source=self.source)

    def get_funnel_result(self):
        if not self.ids:
            self.get_base_ids()

        funnel = [{
            'id': 'base_user',
            'name': '基准用户',
            'value': len(self.ids)
        }]
        for item in self.funnel:
            f_item = {
                'id': item['id'],
                'name': item['name'],
                'value': self.get_value(item)
            }
            funnel.append(f_item)

        # count conversion
        if self.conversion:
            base_count = len(self.ids)
            for item in funnel:
                print(item)
                print('{} / {}'.format(item['value'], base_count))
                item['conversion'] = '{} %'.format(
                    str(round(item['value'] * 100 / base_count, 2)))
                base_count = item['value']

        return funnel

    def get_value(self, item):
        value = 0
        if item['source'] == 'iqg_ro':
            code = item['code'].replace('{ids}',
                                        '({})'.format(', '.join(self.ids)))
            value = self.get_mysql_result(item['source'], code)['data'][0][0]
        return value
