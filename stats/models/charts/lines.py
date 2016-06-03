import os

import arrow
import yaml

from ... import app
from ..stats_base import StatsBase


class LineItem(StatsBase):
    line_path = '{dir}/{file}'.format(
        dir=app.basedir,
        file=app.config['LINE_PATH'])
    attrs = ('title', 'route', 'author',
             'access', 'lines', 'x_axis')
    colors = (
        "rgba(245,166,35,1)",  # orange
        "rgba(92,155,228,1)",  # blue
        "rgba(234,64,64,1)",  # red
        "rgba(132,182,76,1)",  # green
        "rgba(201,33,235,1)",  # purple
        "rgba(0,0,0,1)",  # black
        "rgba(248,231,28,1)"  # yellow
    )

    def __init__(self, name, x_axis=None):
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
                    today = arrow.now('Asia/Shanghai')
                    start = today.replace(days=-30)
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
            if i < len(self.colors):
                line['color'] = self.colors[i]
            else:
                from random import randint
                line['color'] = "rgba({},{},{},1)".format(randint(0, 255),
                                                          randint(0, 255),
                                                          randint(0, 255))
            if line['source'] == 'iqg_mongo':
                line['data'] = self.get_line_mongo_result(line)
            else:
                raise self.AppError('UNKNOWN_SOURCE', source=self.source)
        return self.lines

    def get_line_mongo_result(self, line):
        result = []
        for x in self.x_axis_value:
            code = line['code'].replace('{x_value}', '"{}"'.format(x))
            result.append(self.get_mysql_result_count(line['source'], code))

        return result
