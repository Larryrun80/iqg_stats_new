import os
import zlib

import yaml

from .stats_base import StatsBase
from ..utils.security import ts


class FunnelItem(StatsBase):
    def __init__(self, name, funnel_path=None):
        if not funnel_path:
            funnel_path = '{dir}/{file}'.format(
                dir=self.basedir,
                file=self.app_configs['FUNNEL_PATH'])
        self.attrs += ['conversion', 'base_ids', 'funnel']
        self.ids = []
        for attr in self.attrs:
            setattr(self, attr, None)

        if os.path.exists(funnel_path):
            with open(funnel_path, encoding='utf-8') as f:
                yaml_data = yaml.load(f)

            for y_item in yaml_data:
                if y_item['route'] == name:
                    for attr in self.attrs:
                        setattr(self, attr, y_item.get(attr, None))
                    break
        else:
            raise self.AppError('FILE_NOT_FOUND', file=funnel_path)

    def get_base_ids(self):
        if self.base_ids['source'] and self.base_ids['ids']:
            if self.base_ids['source'] == 'list':
                if not isinstance(self.base_ids['ids'], list):
                    raise self.AppError('TYPE_ERROR',
                                        param=self.base_ids['ids'],
                                        expect_type='list')
                for uid in self.base_ids['ids']:
                    self.ids.append(str(uid))
            else:
                data = self.get_data(self.base_ids['source'],
                                     self.base_ids['ids'])
                for row in data['data']:
                    self.ids.append(str(row[0]))

    def get_funnel_result(self):
        if not self.ids and self.base_ids['source']:
            self.get_base_ids()

        funnel = []
        for item in self.funnel:
            f_item = {
                'id': item['id'],
                'name': item['name'],
                'value': self.get_value(item),
                'export': '',
            }
            if 'type' in item.keys() and item['type'] == 'items':
                export = {
                    'code': item['code'],
                    'source': item['source']
                }
                tsed = ts.dumps(export, salt=self.app_configs['CODE_SALT'])
                f_item['export'] = zlib.compress(tsed.encode(), 9)
            funnel.append(f_item)
        # count conversion
        if self.conversion:
            base_count = None
            for item in funnel:
                if base_count is None:
                    base_count = item['value']
                item['conversion'] = '{} %'.format(
                    str(round(item['value'] * 100 / base_count, 2)))
                base_count = item['value']

        return funnel

    def get_value(self, item):
        if item['source'] == 'list':
            if not isinstance(item['code'], list):
                raise self.AppError('TYPE_ERROR', param=item['code'],
                                    expect_type='list')
            return len(item['code'])

        if '{ids}' in item['code']:
            if not self.ids:
                raise self.AppError('NO_BASE_ID', itemid=item['id'])
            exec_code = item['code'].replace(
                '{ids}', '({})'.format(', '.join(self.ids)))

        if 'type' in item.keys() and item['type'] == 'items':
            value = len(self.get_data(item['source'], exec_code)['data'])
        else:
            value = self.get_data_count(item['source'], exec_code)
        return value
