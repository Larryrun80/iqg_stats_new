import os

import arrow
import yaml

from ... import app
from ..stats_base import StatsBase


class HighLineItem(StatsBase):
    line_path = '{dir}/{file}'.format(
        dir=app.basedir,
        file=app.config['HIGH_LINE_PATH'])

    def __init__(self, name, x_axis=None):
        self.attrs += ['lines', 'x_axis', 'y_axis']
        for attr in self.attrs:
            setattr(self, attr, None)
        self.x_axis_value = x_axis  # 真实的x轴值，list

        if os.path.exists(self.line_path):
            with open(self.line_path, encoding='utf-8') as f:
                yaml_data = yaml.load(f)

            for y_item in yaml_data:
                if y_item['route'] == name:
                    for attr in self.attrs:
                        setattr(self, attr, y_item.get(attr, None))
                    break
        else:
            raise self.AppError('FILE_NOT_FOUND', file=self.line_path)

    def get_x_values(self):
        if not self.x_axis_value:
            if self.x_axis and isinstance(self.x_axis, dict)\
               and 'type' in self.x_axis.keys():
                if self.x_axis['type'] == 'date':
                    duration = self.x_axis.get('duration', 15)
                    today = arrow.now('Asia/Shanghai')
                    start = today.replace(days=-duration)
                    end = today.replace(days=-1)
                    xs = []
                    for r in arrow.Arrow.range('day', start, end):
                        xs.append(r.format('YYYY-MM-DD'))
                    self.x_axis_value = xs
            else:
                raise self.AppError('NO_X_AXIS')

    def get_result(self):
        if not self.x_axis_value:
            self.get_x_values()

        for i, line in enumerate(self.lines, 0):
            if line['source'] == 'iqg_mongo':
                line['data'] = self.get_line_mongo_result(line)
            elif line['source'] in ('', 'iqg_ro', 'hsq_ro'):
                line['data'] = self.get_line_mysql_result(line)
            else:
                raise self.AppError('UNKNOWN_SOURCE', source=self.source)
        return self.lines

    def get_line_mongo_result(self, line):
        result = []
        for x in self.x_axis_value:
            code = line['code'].replace('{x_value}', '"{}"'.format(x))
            result.append(self.get_mongo_result_count(line['source'], code))

        return result

    def get_line_mysql_result(self, line):
        result = []
        for x in self.x_axis_value:
            code = line['code'].replace('{x_value}', '"{}"'.format(x))
            result.append(self.get_mysql_result_count(line['source'], code))

        return result
