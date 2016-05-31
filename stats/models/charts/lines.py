import json
import os
import re

import arrow
import yaml

from ... import app
from ..error import AppError
from ..mongo import init_mongo


class LineItem():
    query_path = '{dir}/{file}'.format(
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
                raise AppError('NO_X_AXIS')

    def get_result(self):
        for i, line in enumerate(self.lines, 0):
            if i < len(self.colors):
                line['color'] = self.colors[i]
            else:
                from random import randint
                line['color'] = "rgba({},{},{},1)".format(randint(0, 255),
                                                          randint(0, 255),
                                                          randint(0, 255))
            if line['source'] == 'iqg_mongo':
                line['data'] = self.get_mongo_result(line)
            else:
                raise AppError('UNKNOWN_SOURCE', source=self.source)
        return self.lines

    def get_mongo_result(self, line):
        if not self.x_axis_value:
            self.get_x_values()

        cnx = init_mongo(line['source'])
        codes = line['code'].split('.')
        if len(codes) < 3:
            raise AppError('WRONG_CODE', code=line['code'])
        collection = cnx[codes[0]][codes[1]]
        condition_pattern = r'find\({.*\}\)'
        condition = re.findall(condition_pattern, codes[2])
        if condition:
            condition = condition[0][5:-1]
        else:
            raise AppError('INVALID_QUERY', query=line['code'])

        result = []
        for x in self.x_axis_value:
            cdt = condition.replace('{x_value}', '"{}"'.format(x))
            cdt = json.loads(cdt)
            result.append(collection.find(cdt).count())
        cnx.close()
        return result
