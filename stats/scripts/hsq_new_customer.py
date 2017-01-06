#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_new_customer.py

import os
import sys

import arrow
import pymongo

CONFIG_STATS_SECTION = 'STATS_MYSQL'
DEFAULT_START_DATE = '2017-01-01'


def get_start_date(cnx):
    collection = cnx['hsq_stats']['new_customers']
    dd = collection.find()\
                   .sort('timestamp', pymongo.DESCENDING)\
                   .limit(1)

    if dd.count() == 0:
        return arrow.get(DEFAULT_START_DATE, 'YYYY-MM-DD')
    else:
        return arrow.get(dd[0]['date'], 'YYYY-MM-DD').replace(days=1)


def get_total_cnt(cnx, cnt_date):
    ''' get total customer count of cnt_date

        arguments:
        cnx: connection of stats database
        cnt_date: date to get count data, with format YYYY-MM-DD

        return:
        distinct user count
    '''
    sql = '''
            select count(distinct(user_id))
            from hsq_order_dealed_new
            where order_at>'{cnt_date} 00:00:00'
            and order_at<='{cnt_date} 23:59:59'
            and user_id not in (
                select user_id
                from hsq_order_dealed_new
                where order_at<'{cnt_date} 00:00:00'
                )
          '''.format(cnt_date=cnt_date)

    cursor = cnx.cursor()
    cursor.execute(sql)
    user_cnt = cursor.fetchone()
    cursor.close()

    return user_cnt[0]


def get_distinct_user_cnt(orders):
    ''' receive an order table (result of select sql) and return distinct user cnt

        arguments:
        orders, a result from sql select, with user_id the first col

        return:
        user count
    '''
    if orders:
        users = [u[0] for u in orders]
        users = set(users)
        return len(users)
    else:
        return 0


def get_normal_orders(cnx, cnt_date):
    ''' get normal customer orders, exclude weishang orders
        exclude the orders whose consignee phone not equal with user phone

        arguments:
        cnx: connection of stats database
        cnt_date: date to get count data, with format YYYY-MM-DD

        return:
        a tuple ([list of column name], (selected data))
    '''
    sql = '''
        select      user_id,
                    register_at,
                    source,
                    device_type,
                    coupon_id,
                    coupon,
                    channel,
                    round(if(sp.settlement_price,
                             o.sku_total_price-o.settlement_price,
                             o.sku_total_price*m.service_rate)/100,2) profit
        from        hsq_order_dealed_new o
        inner join  hsq_sku_promotion_backup sp on o.sku_id=sp.sku_id
        inner join  hsq_merchant_backup m on m.id=o.merchant_id
        where       (mobile='' or mobile=consignee_phone)
        and         order_at>'{cnt_date} 00:00:00'
        and         order_at<='{cnt_date} 23:59:59'
        and         user_id not in (
                        select user_id
                        from hsq_order_dealed_new
                        where order_at<'{cnt_date} 00:00:00'
                        )
    '''.format(cnt_date=cnt_date)

    cursor = cnx.cursor()
    cursor.execute(sql)
    columns = [i[0] for i in cursor.description]
    orders = cursor.fetchall()
    cursor.close()

    return (columns, orders)


def get_group_orders(orders):
    ''' get all normal group orders

        arguments:
        orders, format with ([column_names], (selected data))

        return:
        ([orders of normal skus], [orders of promotion skus])
    '''
    if len(orders) != 2:
        raise RuntimeError('Invalid order info passed')

    source_column = 'source'
    profit_column = 'profit'
    source_seq = 0
    profit_seq = 0
    for i, cn in enumerate(orders[0], 0):
        if cn == source_column:
            source_seq = i

        if cn == profit_column:
            profit_seq = i

    normal_orders = [row for row in orders[1]
                     if (row[source_seq] == 0 and row[profit_seq] >= 0)]
    promotion_orders = [row for row in orders[1]
                        if (row[source_seq] == 0 and row[profit_seq] < 0)]
    return (normal_orders, promotion_orders)


def get_jx_orders(orders):
    ''' get all normal jingxuan orders

        arguments:
        orders, format with ([column_names], (selected data))

        return:
        ([orders of normal skus], [orders of promotion skus])
    '''
    if len(orders) != 2:
        raise RuntimeError('Invalid order info passed')

    source_column = 'source'
    profit_column = 'profit'
    source_seq = 0
    profit_seq = 0
    for i, cn in enumerate(orders[0], 0):
        if cn == source_column:
            source_seq = i

        if cn == profit_column:
            profit_seq = i

    normal_orders = [row for row in orders[1]
                     if (row[source_seq] == 2 and row[profit_seq] >= 0)]
    promotion_orders = [row for row in orders[1]
                        if (row[source_seq] == 2 and row[profit_seq] < 0)]
    return (normal_orders, promotion_orders)


def get_app_static(cnx, orders):
    ''' get app orders by coupon

        arguments:
        orders, format with ([column_names], (selected data))

        return:
        {
            'total': xxx,
            'taobao': xxx,
            'nature': { channel info },
            'campaign': { campaign info }
        }
        all weishang coupon orders will be gathered to taobao order
        all orders didn't use coupon or used newbie coupons will in nature
        all orders used campaign coupon will in campaign
    '''
    if len(orders) != 2:
        raise RuntimeError('Invalid order info passed')

    return_data = {
        'total': 0,
        'taobao': 0,
        'nature': {},
        'campaign': {}
    }

    taobao_channel = '微商'
    nature_reward_ids = (0, 1, 441)

    source_column = 'source'
    coupon_column = 'coupon'
    couponid_column = 'coupon_id'
    channel_column = 'channel'
    source_seq = 0
    couponid_seq = 0
    coupon_seq = 0
    channel_seq = 0
    for i, cn in enumerate(orders[0], 0):
        if cn == source_column:
            source_seq = i

        if cn == couponid_column:
            couponid_seq = i

        if cn == coupon_column:
            coupon_seq = i

        if cn == channel_column:
            channel_seq = i

    # add reward infos to orders
    tmp_orders = []

    sql = '''
            select reward_id, channel, maintainer
            from hsq_coupon_management
            where coupon_id={}
    '''

    cursor = cnx.cursor()
    for row in orders[1]:
        if row[source_seq] == 1:
            if row[couponid_seq] != 0:
                c_sql = sql.format(row[couponid_seq])
                cursor.execute(c_sql)
                c_info = cursor.fetchone()
                if c_info:
                    append = list(c_info)
                    tmp_orders.append(list(row) + append)
                else:
                    tmp_orders.append(list(row) + [-1, 'unknown', 'unknown'])
            else:
                tmp_orders.append(list(row) + [0, None, None])
    cursor.close()

    return_data['total'] = get_distinct_user_cnt(orders[1])

    taobao_orders = [row for row in tmp_orders
                     if row[-2] == taobao_channel]
    return_data['taobao'] = get_distinct_user_cnt(taobao_orders)

    nature_orders = [row for row in tmp_orders
                     if row[-3] in nature_reward_ids]
    channel_orders = {}
    for row in nature_orders:
        if row[channel_seq] not in channel_orders.keys():
            channel_orders[row[channel_seq]] = [row]
        else:
            channel_orders[row[channel_seq]] += [row]
    return_data['nature']['total'] = get_distinct_user_cnt(nature_orders)
    for k, v in channel_orders.items():
        return_data['nature'][k] = get_distinct_user_cnt(v)

    campaign_orders = [row for row in tmp_orders
                       if (row[-3] not in nature_reward_ids and
                           row[-2] != taobao_channel)]
    cc_orders = {}
    for row in campaign_orders:
        if row[coupon_seq] not in cc_orders.keys():
            cc_orders[coupon_seq] = [row]
        else:
            cc_orders[coupon_seq] += [row]
    return_data['campaign']['total'] = get_distinct_user_cnt(campaign_orders)
    for k, v in cc_orders.items():
        return_data['campaign']['{} - {}'.format(v[0][-2], k)] = \
            get_distinct_user_cnt(v)

    return return_data


# def get_app_coupon_orders(orders):
#     ''' get app orders by coupon

#         arguments:
#         orders, format with ([column_names], (selected data))

#         return:
#         {'coupon1': (orders1), 'coupon2': (orders2)}
#     '''
#     if len(orders) != 2:
#         raise RuntimeError('Invalid order info passed')

#     return_data = {}
#     source_column = 'source'
#     coupon_column = 'coupon'
#     source_seq = 0
#     coupon_seq = 0
#     for i, cn in enumerate(orders[0], 0):
#         if cn == source_column:
#             source_seq = i

#         if cn == coupon_column:
#             coupon_seq = i

#     for row in orders[1]:
#         if row[source_seq] == 1:
#             if row[coupon_seq] not in return_data.keys():
#                 return_data[row[coupon_seq]] = [row]
#             else:
#                 return_data[row[coupon_seq]] += [row]

#     return return_data


# def get_app_channel_orders(orders):
#     ''' get app orders by channel

#         arguments:
#         orders, format with ([column_names], (selected data))

#         return:
#         {'channel1': (orders1), 'channel2': (orders2)}
#     '''
#     if len(orders) != 2:
#         raise RuntimeError('Invalid order info passed')

#     return_data = {}
#     source_column = 'source'
#     coupon_column = 'channel'
#     source_seq = 0
#     coupon_seq = 0
#     for i, cn in enumerate(orders[0], 0):
#         if cn == source_column:
#             source_seq = i

#         if cn == coupon_column:
#             coupon_seq = i

#     for row in orders[1]:
#         if row[source_seq] == 1:
#             if row[coupon_seq] not in return_data.keys():
#                 return_data[row[coupon_seq]] = [row]
#             else:
#                 return_data[row[coupon_seq]] += [row]

#     return return_data


def write_mongo(cnx, data):
    db = 'hsq_stats'
    collection = 'new_customers'
    new_customer_data = cnx[db][collection]
    new_customer_data.insert_one(data)


if __name__ == '__main__':
    import_path = '{}/../../'.format(
        os.path.abspath(os.path.dirname(__file__)))
    sys.path.append(import_path)

    from stats.models.mysql import init_mysql
    from stats.models.mongo import init_mongo
    from script_log import print_log

    try:
        stats_cnx = init_mysql()
        mongo_cnx = init_mongo('HSQ_STATS_MONGO')

        print_log('Start...')
        start = get_start_date(mongo_cnx)
        # start = arrow.get(DEFAULT_START_DATE, 'YYYY-MM-DD')

        end = arrow.now('Asia/Shanghai').replace(days=-1)

        for r in arrow.Arrow.range('day', start, end):
            print_log('dealing date {}'.format(r.format('YYYY-MM-DD')))
            date_data = {
                'date': r.format('YYYY-MM-DD'),
                'timestamp': arrow.now('Asia/Shanghai').timestamp,
            }  # to store results

            # get total customer count
            total = get_total_cnt(stats_cnx, r.format('YYYY-MM-DD'))
            date_data['total'] = total

            # get order list exclude weishang orders
            orders = get_normal_orders(stats_cnx, r.format('YYYY-MM-DD'))
            date_data['weishang'] = total - get_distinct_user_cnt(orders[1])

            # group orders
            group_orders = get_group_orders(orders)
            date_data['group_normal'] = get_distinct_user_cnt(group_orders[0])
            date_data['group_promotion'] = \
                get_distinct_user_cnt(group_orders[1])

            # jingxuan orders
            jx_orders = get_jx_orders(orders)
            date_data['jx_normal'] = get_distinct_user_cnt(jx_orders[0])
            date_data['jx_promotion'] = get_distinct_user_cnt(jx_orders[1])

            # # app orders via coupon
            # app_coupon_data = {}
            # coupon_orders = get_app_coupon_orders(orders)
            # for c in coupon_orders.keys():
            #     app_coupon_data[c] = get_distinct_user_cnt(coupon_orders[c])
            # date_data['app_coupons'] = app_coupon_data

            # # app orders via channel
            # app_channel_data = {}
            # channel_orders = get_app_channel_orders(orders)
            # for c in channel_orders.keys():
            #     app_channel_data[c] = get_distinct_user_cnt(channel_orders[c])
            # date_data['app_channels'] = app_channel_data

            # app orders
            date_data['app'] = get_app_static(stats_cnx, orders)

            # write to mongo
            write_mongo(mongo_cnx, date_data)

        print_log('Done!')
    except Exception as e:
        print_log(e, 'ERROR')
    finally:
        stats_cnx.close()
        mongo_cnx.close()
