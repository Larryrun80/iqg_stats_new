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


def get_distinct_users(orders):
    ''' receive an order table (result of select sql) and return distinct user cnt

        arguments:
        orders, a result from sql select, with user_id the first col

        return:
        user count
    '''
    if orders:
        users = [u[0] for u in orders]
        return set(users)
    else:
        return set()


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
                    platform_coupon_id coupon_id,
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


def get_group_statics(orders, dealed_users):
    ''' get all normal group orders

        arguments:
        orders, format with ([column_names], (selected data))

        return:
        ([orders of normal skus], [orders of promotion skus])
    '''
    if len(orders) != 2:
        raise RuntimeError('Invalid order info passed')

    return_data = {}
    cols_seq = {
                    'user_id': 0,
                    'source': 0,
                    'profit': 0,
    }

    for i, cn in enumerate(orders[0], 0):
        for col in cols_seq:
            if col == cn:
                cols_seq[col] = i

    promotion_orders = [row for row in orders[1]
                        if (row[cols_seq['source']] == 0 and
                            row[cols_seq['profit']] < 0 and
                            row[cols_seq['user_id']] not in dealed_users)]
    p_users = get_distinct_users(promotion_orders)
    dealed_users = dealed_users | p_users

    normal_orders = [row for row in orders[1]
                     if (row[cols_seq['source']] == 0 and
                         row[cols_seq['profit']] >= 0 and
                         row[cols_seq['user_id']] not in dealed_users)]
    n_users = get_distinct_users(normal_orders)
    dealed_users = dealed_users | n_users

    return_data['group_normal'] = len(n_users)
    return_data['group_promotion'] = len(p_users)
    return_data['total'] = len(n_users | p_users)
    return (return_data, dealed_users)


def get_jx_statics(orders, dealed_users):
    ''' get all normal jingxuan orders

        arguments:
        orders, format with ([column_names], (selected data))

        return:
        ([orders of normal skus], [orders of promotion skus])
    '''
    if len(orders) != 2:
        raise RuntimeError('Invalid order info passed')

    return_data = {}
    cols_seq = {
                    'user_id': 0,
                    'source': 0,
                    'profit': 0,
    }

    for i, cn in enumerate(orders[0], 0):
        for col in cols_seq:
            if col == cn:
                cols_seq[col] = i

    promotion_orders = [row for row in orders[1]
                        if (row[cols_seq['source']] == 2 and
                            row[cols_seq['profit']] < 0 and
                            row[cols_seq['user_id']] not in dealed_users)]
    p_users = get_distinct_users(promotion_orders)
    dealed_users = dealed_users | p_users

    normal_orders = [row for row in orders[1]
                     if (row[cols_seq['source']] == 2 and
                         row[cols_seq['profit']] >= 0 and
                         row[cols_seq['user_id']] not in dealed_users)]
    n_users = get_distinct_users(normal_orders)
    dealed_users = dealed_users | n_users

    return_data['jx_normal'] = len(n_users)
    return_data['jx_promotion'] = len(p_users)
    return_data['total'] = len(n_users | p_users)
    return (return_data, dealed_users)


def get_app_static(cnx, orders, dealed_users):
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

    cols_seq = {
                'user_id':  0,
                'source': 0,
                'coupon': 0,
                'coupon_id': 0,
                'channel': 0,
    }

    for i, cn in enumerate(orders[0], 0):
        for col in cols_seq.keys():
            if cn == col:
                cols_seq[col] = i

    # add reward infos to orders
    tmp_orders = []

    sql = '''
            select reward_id, channel, maintainer
            from hsq_coupon_management
            where coupon_id={}
    '''

    cursor = cnx.cursor()
    for row in orders[1]:
        if row[cols_seq['source']] == 1:
            if row[cols_seq['coupon_id']] != 0:
                c_sql = sql.format(row[cols_seq['coupon_id']])
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

    return_data['total'] = len(get_distinct_users(tmp_orders))

    taobao_orders = [row for row in tmp_orders
                     if row[-2] == taobao_channel]
    dealed_users = get_distinct_users(taobao_orders)
    return_data['taobao'] = len(dealed_users)

    # nature orders
    nature_orders = [row for row in tmp_orders
                     if (row[-3] in nature_reward_ids and
                         row[cols_seq['user_id']] not in dealed_users)]
    channel_orders = {}
    for row in nature_orders:
        if row[cols_seq['channel']] not in channel_orders.keys():
            channel_orders[row[cols_seq['channel']]] = [row]
        else:
            channel_orders[row[cols_seq['channel']]] += [row]

    nature_users = get_distinct_users(nature_orders)
    dealed_users = dealed_users | nature_users
    return_data['nature']['total'] = len(nature_users)
    for k, v in channel_orders.items():
        return_data['nature'][k] = len(get_distinct_users(v))

    # campaign_orders
    campaign_orders = [row for row in tmp_orders
                       if (row[-3] not in nature_reward_ids and
                           row[-2] != taobao_channel and
                           row[cols_seq['user_id']] not in dealed_users)]
    cc_orders = {}
    for row in campaign_orders:
        if row[cols_seq['coupon']] not in cc_orders.keys():
            cc_orders[row[cols_seq['coupon']]] = [row]
        else:
            cc_orders[row[cols_seq['coupon']]] += [row]
    campaign_users = get_distinct_users(campaign_orders)
    dealed_users = dealed_users | campaign_users
    return_data['campaign']['total'] = len(campaign_users)
    for k, v in cc_orders.items():
        return_data['campaign']['{} - {}'.format(v[0][-2], k)] = \
           len(get_distinct_users(v))

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
        dealed_users = set()

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
            date_data['weishang'] = total - len(get_distinct_users(orders[1]))

            # group orders
            (group_statics, dealed_users) = get_group_statics(
                                                orders, dealed_users)
            date_data['group'] = group_statics

            # jingxuan orders
            (jx_statics, dealed_users) = get_jx_statics(orders, dealed_users)
            date_data['jx'] = jx_statics

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
            date_data['app'] = get_app_static(stats_cnx, orders, dealed_users)

            # write to mongo
            write_mongo(mongo_cnx, date_data)

        print_log('Done!')
    except Exception as e:
        print_log(e, 'ERROR')
    finally:
        stats_cnx.close()
        mongo_cnx.close()
