#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_campaign_analyze.py

import os
import sys

import arrow
from tqdm import tqdm


class CampaignAnalyzer():
    """docstring for CampaignAnalyzer"""

    # we will ensure the analyze objects have enough time
    # to act as our customer
    delay_dates = 5

    def __init__(self, campaign_type=1, **kwargs):
        super(CampaignAnalyzer, self).__init__()
        # campaign_type: 1. coupon
        self.campaign_type = campaign_type
        self.deadline = arrow.now('Asia/Shanghai')\
                             .replace(days=-self.delay_dates)
        self.args = kwargs
        self.result = {
            'description': '',
            'total': 0,
            'new_cnt': 0,
            'reperchase': {
                'total': 0,
                'right_day_total': 0,
                'next_day_total': 0,
                'hq': 0,
            },

        }

        # default value
        if self.campaign_type in (3, ):
            self.args['start'] = self.deadline.replace(days=-30)
            self.args['end'] = self.deadline.replace(days=-1)

        self.check_args()

        import_path = '{}/../../'.format(
            os.path.abspath(os.path.dirname(__file__)))
        sys.path.append(import_path)

        from stats.models.mysql import init_mysql
        self.cnx = init_mysql()

    def print_log(self, msg):
        from script_log import print_log
        print_log(msg)

    def __delete__(self):
        self.cnx.close()

    def check_args(self):
        illegal_args = {
            '1':    {'coupon_id': int},
            '2':    {'sku_id': int},
            '3':    {'start': arrow, 'end': arrow},
        }

        if str(self.campaign_type) not in illegal_args.keys():
            raise RuntimeError('unknown campaign type')

        for arg in illegal_args[str(self.campaign_type)]:
            if arg not in self.args.keys() or not self.args[arg]:
                raise RuntimeError('{} not found in campaign'.format(arg))
            # todo: arg type check

    def do_statics(self):
        self.print_log("start analyzing users' info")
        if self.campaign_type == 1:
            self.get_coupon_user_outline()
        if self.campaign_type == 2:
            self.get_lottery_user_outline()
        if self.campaign_type == 3:
            self.get_pin_user_outline()
        if self.campaign_type == 4:
            self.get_normal_user_outline()

        self.print_log("counting reperchase info")
        self.get_repeat_purchase_count()

        self.print_log('-----------')
        self.print_log('  统计结果  ')
        self.print_log('-----------')
        self.print_log(self.result['description'])
        total = self.result['total']
        new_cnt = self.result['new_cnt']
        self.print_log('所有用户: {}'.format(total))
        self.print_log('新客数量: {} ( {} % )'.format(
            self.result['new_cnt'], round(new_cnt/total * 100, 2)))
        self.print_log('复购用户: {} ( {} % )'.format(
            self.result['reperchase']['total'],
            round(self.result['reperchase']['total']/new_cnt * 100, 2)))
        self.print_log('当日复购: {} ( {} % )'.format(
            self.result['reperchase']['right_day_total'],
            round(
                self.result['reperchase']['right_day_total']/new_cnt*100, 2)))
        self.print_log('隔日复购: {} ( {} % )'.format(
            self.result['reperchase']['next_day_total'],
            round(self.result['reperchase']['next_day_total']/new_cnt*100, 2)))
        self.print_log('优质用户: {} ( {} % )'.format(
            self.result['reperchase']['hq'],
            round(self.result['reperchase']['hq']/new_cnt*100, 2)))

    def get_coupon_user_outline(self):
        cursor = self.cnx.cursor()
        sql = '''
                select  user_id,
                        min(order_at) first_order_time,
                        min(order_id) min_oid,
                        min(if(platform_coupon_id={coupon_id},
                               order_id,
                               null)) min_coupon_oid
                from hsq_order_dealed_new
                where user_id in
                    (
                        select user_id
                        from hsq_order_dealed_new
                        where platform_coupon_id={coupon_id}
                    )
                and order_at<'{deadline}'
                group by user_id
        '''.format(coupon_id=self.args['coupon_id'],
                   deadline=self.deadline.format('YYYY-MM-DD'))

        cursor.execute(sql)
        data = cursor.fetchall()

        sql = '''
                select  title
                from    hsq_coupon_management
                where   coupon_id={}
        '''.format(self.args['coupon_id'])
        cursor.execute(sql)
        coupon = cursor.fetchone()

        cursor.close()

        if not coupon:
            raise RuntimeError('coupon id {} not found in database'.format(
                self.args['coupon_id']))

        self.result['description'] = \
            '使用优惠券 {} 的用户, 基于 {} 前下单数据的统计'.format(
                coupon[0], self.deadline.format('YYYY-MM-DD'))

        self.result['total'] = len(data)
        new_users = []
        for user in data:
            if user[-1] == user[-2]:
                new_users.append({
                    'uid': user[0],
                    'first_order_time': arrow.get(user[1])
                })
        self.result['new_cnt'] = len(new_users)

        self.users = new_users

    def get_lottery_user_outline(self):
        cursor = self.cnx.cursor()
        sql = '''
                select  user_id,
                        min(order_at) first_order_time,
                        min(order_id) min_oid,
                        min(if(source=-1
                               and sku_id={sku_id},
                               order_id,
                               null)) min_lottery_oid
                from hsq_order_dealed_new
                where user_id in
                    (
                        select user_id
                        from hsq_order_dealed_new
                        where sku_id={sku_id}
                        and source=-1
                    )
                and order_at<'{deadline}'
                group by user_id
        '''.format(sku_id=self.args['sku_id'],
                   deadline=self.deadline.format('YYYY-MM-DD'))

        cursor.execute(sql)
        data = cursor.fetchall()

        sql = '''
                select  sku
                from    hsq_order_dealed_new
                where   source=-1
                and     sku_id={}
                limit   1
        '''.format(self.args['sku_id'])
        cursor.execute(sql)
        sku = cursor.fetchone()
        cursor.close()

        self.result['description'] = \
            '参加 {} 抽奖的用户, 基于 {} 前下单数据的统计'.format(
                sku[0], self.deadline.format('YYYY-MM-DD'))

        self.result['total'] = len(data)
        new_users = []
        for user in data:
            if user[-1] == user[-2]:
                new_users.append({
                    'uid': user[0],
                    'first_order_time': arrow.get(user[1])
                })
        self.result['new_cnt'] = len(new_users)

        self.users = new_users

    def get_pin_user_outline(self):
        cursor = self.cnx.cursor()
        sql = '''
                select  user_id,
                        min(order_at) first_order_time,
                        min(order_id) min_oid,
                        min(if(source=0
                               and settlement_price<=sku_total_price
                               and order_at>'{start}'
                               and order_at<'{end}',
                               order_id,
                               null)) min_pin_oid
                from hsq_order_dealed_new
                where user_id in
                    (
                        select user_id
                        from hsq_order_dealed_new
                        where source=0
                        and settlement_price<=sku_total_price
                        and order_at>'{start}'
                        and order_at<'{end}'
                    )
                and order_at<'{deadline}'
                group by user_id
        '''.format(start=self.args['start'].format('YYYY-MM-DD'),
                   end=self.args['end'].replace(days=1).format('YYYY-MM-DD'),
                   deadline=self.deadline.format('YYYY-MM-DD'))
        # print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()

        self.result['description'] = \
            '基于 {} ~ {} 普通拼团用户的统计'.format(
                self.args['start'].format('YYYY-MM-DD'),
                self.args['end'].format('YYYY-MM-DD'))

        self.result['total'] = len(data)
        new_users = []
        for user in data:
            if user[-1] == user[-2]:
                new_users.append({
                    'uid': user[0],
                    'first_order_time': arrow.get(user[1])
                })
        self.result['new_cnt'] = len(new_users)

        self.users = new_users

    def get_normal_user_outline(self):
        cursor = self.cnx.cursor()
        sql = '''
                select  user_id,
                        min(order_at) first_order_time,
                        min(order_id) min_oid,
                        min(if(source=1
                               and `platform_coupon_id`=0
                               and order_at>'{start}'
                               and order_at<'{end}',
                               order_id,
                               null)) min_pin_oid
                from hsq_order_dealed_new
                where user_id in
                    (
                        select user_id
                        from hsq_order_dealed_new
                        where source=1
                        and `platform_coupon_id`=0
                        and order_at>'{start}'
                        and order_at<'{end}'
                    )
                and order_at<'{deadline}'
                group by user_id
        '''.format(start=self.args['start'].format('YYYY-MM-DD'),
                   end=self.args['end'].replace(days=1).format('YYYY-MM-DD'),
                   deadline=self.deadline.format('YYYY-MM-DD'))
        # print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()

        self.result['description'] = \
            '基于 {} ~ {} 普通购买用户的统计'.format(
                self.args['start'].format('YYYY-MM-DD'),
                self.args['end'].format('YYYY-MM-DD'))

        self.result['total'] = len(data)
        new_users = []
        for user in data:
            if user[-1] == user[-2]:
                new_users.append({
                    'uid': user[0],
                    'first_order_time': arrow.get(user[1])
                })
        self.result['new_cnt'] = len(new_users)

        self.users = new_users

    def get_repeat_purchase_count(self):
        cursor = self.cnx.cursor()
        for user in tqdm(self.users):
            sql = '''
                    select  order_id,
                            pay_id,
                            order_at,
                            platform_coupon_id,
                            source,
                            sku_total_price,
                            settlement_price,
                            platform_discount
                    from hsq_order_dealed_new
                    where user_id={uid}
                    and order_at<'{order_deadline}'
                    and sku_total_price
            '''.format(uid=user['uid'],
                       order_deadline=user['first_order_time'].replace(
                        days=self.delay_dates).format('YYYY-MM-DD'))

            # print(sql)
            cursor.execute(sql)
            orders = cursor.fetchall()
            pay_ids = []
            order_days = []
            hq_flag = False
            nex_day = False

            for o in orders:
                if o[1] not in pay_ids:
                    pay_ids.append(o[1])
                    order_days.append(arrow.get(o[2]).format('YYYY-MM-DD'))
                # if o[3] == 0 and o[5] >= o[6] and o[4] >= 0:
                if o[5] - o[7] > 500:
                    hq_flag = True

            if len(pay_ids) > 1:
                self.result['reperchase']['total'] += 1
                if hq_flag:
                    self.result['reperchase']['hq'] += 1

                fd_count = 0
                for t in order_days:
                    if t == user['first_order_time'].format('YYYY-MM-DD'):
                        fd_count += 1
                    else:
                        nex_day = True

                if fd_count > 1:
                    self.result['reperchase']['right_day_total'] += 1

                if nex_day:
                    self.result['reperchase']['next_day_total'] += 1

        cursor.close()


def makeChoice():
    campaign_type = input('''请用数字键选择查询类型：
1. 根据优惠券id查询
2. 根据抽奖团sku id查询
3. 常规拼团用户
4. 常规购买用户

''')
    while campaign_type not in ('1', '2', '3'):
        campaign_type = input('请输入序号： ')

    if campaign_type == '1':
        coupon_id = input('请输入优惠券id: ')
        return CampaignAnalyzer(1, coupon_id=coupon_id)

    if campaign_type == '2':
        sku_id = input('请输入抽奖团 sku id: ')
        return CampaignAnalyzer(2, sku_id=sku_id)

    if campaign_type in ('3', '4'):
        ca = CampaignAnalyzer(3)
        print('请输入查询时间段，直接回车默认查询过去30天的常规拼团用户')
        start_date = input('请以 YYYY-MM-DD 格式，输入开始时间 [{}]:'.format(
            ca.args['start'].format('YYYY-MM-DD')))
        while True:
            if start_date == '':
                break
            else:
                try:
                    start_date = arrow.get(start_date, 'YYYY-MM-DD')
                    break
                except:
                    start_date = input(
                        '请以 YYYY-MM-DD 格式，输入开始时间 [{}]:'.format(
                            ca.args['end'].format('YYYY-MM-DD')))
        end_date = input('请以 YYYY-MM-DD 格式，输入结束时间 [{}]:'.format(
            ca.args['end'].format('YYYY-MM-DD')))
        while True:
            if end_date == '':
                break
            else:
                try:
                    end_date = arrow.get(end_date, 'YYYY-MM-DD')
                    break
                except:
                    end_date = input(
                        '请以 YYYY-MM-DD 格式，输入结束时间 [{}]:'.format(
                            ca.args['end'].format('YYYY-MM-DD')))

        if start_date:
            ca.args['start'] = arrow.get(start_date.format('YYYY-MM-DD'))
        if end_date:
            ca.args['end'] = arrow.get(end_date.format('YYYY-MM-DD'))

        return ca


if __name__ == '__main__':
    try:
        ca = makeChoice()
        ca.do_statics()
        ca.print_log('Done!')
    except KeyboardInterrupt or EOFError:
        print()
        print('bye...')
    except Exception as e:
        print(e)
