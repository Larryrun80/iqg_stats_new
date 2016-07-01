import csv
import os

import arrow
import xlwt
import yaml

from .stats_base import StatsBase
from ..utils.security import ts


class ExportData(StatsBase):
    result = {
        'url': '',
        'message': ''
    }

    def __init__(self, file_type, cid):
        self.file_type = file_type
        self.cid = cid

    def get_file(self):
        export_path = self.app_configs['EXPORT_CMD_PATH']
        real_path = '{}/{}'.format(self.basedir, export_path)
        yaml_in_time = []

        with open(real_path, encoding='utf-8') as f:
            yaml_data = list(yaml.load(f))

        timeouted = arrow.now().timestamp - 3600
        # del timeouted items
        for item in yaml_data:
            if item['time'] >= timeouted:
                yaml_in_time.append(item)

        with open(real_path, mode='w', encoding='utf-8') as f:
            if yaml_in_time:
                f.write(yaml.dump(yaml_in_time))
            else:
                pass

        for item in yaml_in_time:
            if item['cid'] == self.cid:
                try:
                    source = item['source']
                    code = ts.loads(item['code'],
                                    salt=self.app_configs['CODE_SALT'])
                except:
                    self.result['message'] = '文件不存在或超时，请重试'
                    return self.result

                data = self.get_data(source, code)
                data = [list(data['columns'])] + list(data['data'])
                self.result['url'] = self.generate_file(data)
                self.result['message'] = 'success'
                return self.result

        self.result['message'] = '文件不存在或超时，请重试'
        return self.result

    def generate_file(self, data, export_dir=''):
        '''
            generate correspoding format file using data
        '''
        if not export_dir:
            export_dir = 'tmp/'
        real_path = '{}/static/{}'.format(self.basedir, export_dir)

        if not os.path.exists(real_path):
            os.makedirs(real_path)
        tmp_file_name = '{}.{}'.format(
            arrow.now('Asia/Shanghai').format('YYYY-DDD-X'),
            self.file_type)
        save_file_path = real_path + tmp_file_name

        # if the file exists, try to generate another one
        # this step is to avoid download a wrong file
        while os.path.isfile(save_file_path):
            tmp_file_name = '{}.{}'.format(
                arrow.now('Asia/Shanghai').format('YYYY-DDD-X'),
                self.file_type)
            save_file_path = real_path + tmp_file_name

        return_url = '{}{}'.format(export_dir, tmp_file_name)

        if self.file_type == 'xls':
            wb = xlwt.Workbook()
            wb.encoding = 'gbk'
            ws = wb.add_sheet('data')
            for row in range(len(data)):
                for col in range(len(data[0])):
                    ws.write(row, col, data[row][col])

            wb.save(save_file_path)
            return return_url

        if self.file_type == 'csv':
            with open(save_file_path,
                      mode='w',
                      encoding='utf-8',
                      errors='ignore') as target:
                writer = csv.writer(target)
                writer.writerows(data)
            return return_url
