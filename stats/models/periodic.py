import os

import arrow
import yaml

from .. import app
from .stats_base import StatsBase


class PeriodicItem(StatsBase):
    periodic_path = '{dir}/{file}'.format(
        dir=app.basedir,
        file=app.config['PERIOD_PATH'])

    def __init__(self, name):
        self.attrs += ['direction', 'total', 'periods', 'stats']

        for attr in self.attrs:
            setattr(self, attr, None)

        if os.path.exists(self.periodic_path):
            with open(self.periodic_path, encoding='utf-8') as f:
                yaml_data = yaml.load(f)

            for y_item in yaml_data:
                if y_item['route'] == name:
                    for attr in self.attrs:
                        setattr(self, attr, y_item.get(attr, None))
                    break
        else:
            raise self.AppError('FILE_NOT_FOUND', file=self.periodic_path)

    def get_periods(self):
        if not isinstance(self.periods['span'], list):
            raise self.AppError('TYPE_ERROR',
                                param='span of period',
                                expect_type='list')
        if self.periods:
            end_date = arrow.now('Asia/Shanghai')
            unit = 'days'
            if self.periods['unit'] == 'month':
                unit = 'months'
            if self.periods['nature']:
                param = {unit: 1}
                end_date = \
                    end_date.replace(**param).floor(self.periods['unit'])

            periods = []
            for i in reversed(range(len(self.periods['span']))):
                param = {unit: -self.periods['span'][i]}
                start_date = end_date.replace(**param)
                periods.insert(0, (start_date, end_date))
                if not self.periods['accumulate']:
                    end_date = start_date

            return periods
        else:
            return None

    def assemble_data(self):
        data = []
        # create header
        header = ['时段']
        for item in self.stats:
            header.append(item['name'])
        data.append(header)

        # create data
        periods = self.get_periods()
        for period in periods:
            p_data = ['{} - {}'.format(period[0].format('YYYY-MM-DD'),
                                       period[1].format('YYYY-MM-DD'))]
            for item in self.stats:
                p_data.append(self.get_period_cnt(item, period))
            data.append(p_data)

        if self.total:
            total = ['总计']
            for i in range(1, len(header)):
                col_sum = 0
                for d in data[1:]:
                    col_sum += d[i]
                total.append(round(col_sum, 2))
            data.append(total)
        return data

    def get_period_cnt(self, item, period):
        cnt = 0
        if item['source'] == 'iqg_ro':
            sql = item['code']
            sql = sql.replace('{start_date}', period[0].format('YYYY-MM-DD'))
            sql = sql.replace('{end_date}', period[1].format('YYYY-MM-DD'))
            cnt = self.get_mysql_result_count(item['source'], sql)

        return cnt
