import os

import yaml

from .stats_base import StatsBase


class FunnelItem(StatsBase):
    def __init__(self, name):
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
                if isinstance(self.base_ids['ids'], list):
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
                'value': self.get_value(item)
            }
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
        if '{ids}' in item['code']:
            if not self.ids:
                raise self.AppError('NO_BASE_ID', itemid=item['id'])
            item['code'] = item['code'].replace(
                '{ids}', '({})'.format(', '.join(self.ids)))

        value = self.get_data_count(item['source'], item['code'])
        return value
