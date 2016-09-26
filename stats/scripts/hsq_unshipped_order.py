#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_unshipped_order.py

import os
import sys

CONFIG_HSQ_SECTION = 'HSQ_MYSQL'
CONFIG_STATS_SECTION = 'STATS_MYSQL'


def get_columns(cnx):
    sql = 'select * from hsq_order_unshipped limit 1'
    cursor = cnx.cursor()
    cursor.execute(sql)
    cols = [i[0] for i in cursor.description]
    cursor.close()
    return cols


def truncate_stats(cnx):
    sql = 'truncate table hsq_order_unshipped'

    cursor = cnx.cursor()
    cursor.execute(sql)
    cnx.commit()
    cursor.close()


def get_unshipped_order_ids(cnx):
    # get total
    sql = '''
            select o.id, so.id
            from trade_sub_order so
            inner join trade_order o on so.order_id=o.id
            where so.status=2
            and so.created_at<(unix_timestamp(now()) - 3600*24)
            and o.delivery_time is null
            order by o.id asc
          '''

    cursor = cnx.cursor()
    cursor.execute(sql)
    total_ids = cursor.fetchall()

    if total_ids:
        # get refund
        min_oid = total_ids[0][0]

        refund_ids = []
        sql = '''
                select order_id, sub_order_id
                from trade_refund_order
                where refund_status in (2, 3)
                and order_id>={}
              '''.format(min_oid)

        cursor = cnx.cursor()
        cursor.execute(sql)
        refund_ids = cursor.fetchall()
        refund_oids = []
        refund_soids = []
        for idrow in refund_ids:
            if idrow[1] is None:
                if idrow[0] not in refund_oids:
                    refund_oids.append(idrow[0])
            else:
                if idrow[1] not in refund_soids:
                    refund_soids.append(idrow[1])

        # exclude refund
        soids = []
        for idrow in total_ids:
            if idrow[0] not in refund_oids\
              and idrow[1] not in refund_soids:
                soids.append(idrow[1])

    cursor.close()
    return soids


def get_order_detail(cnx, soids):
    for i, soid in enumerate(soids, 0):
        soids[i] = str(soid)

    data = []
    sql = '''
            select so.id so_id,
                   o.id o_id,
                   so.sku_id sku_id,
                   so.sku_name sku,
                   m.name merchant,
                   m.contacter contacter,
                   m.contacter_phone tel,
                   if (so.created_at<(unix_timestamp(now())
                    - 3600*24*2), 1, 0) above_2days,
                   o.`consignee` consignee,
                   o.consignee_phone consignee_tel,
                   o.delivery_province province,
                   o.delivery_city city,
                   from_unixtime(so.created_at) created_at
            from trade_sub_order so
            inner join trade_order o on so.order_id=o.id
            inner join merchant m on m.id=o.merchant_id
            where so.id in ({})
    '''.format(', '.join(soids))

    cursor = cnx.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()

    return data


def insert_data(cnx, cols, data):
    # deal col
    ins_col = '({})'.format(','.join(cols[1:]))

    # deal data to be inserted
    escape_chars = ('\\', '"', "'")
    dealed_data = []
    for d in data:
        if isinstance(d, int):
            dealed_data.append(str(d))
        else:
            sd = str(d)
            for ec in escape_chars:
                if ec in sd:
                    sd = sd.replace(ec, '\{}'.format(ec))
            dealed_data.append('"{}"'.format(sd))

    dealed_data.append('now()')
    ins_val = '({})'.format(','.join(dealed_data))
    sql = '''
            insert into hsq_order_unshipped
            {cols} values {vals}
          '''.format(cols=ins_col, vals=ins_val)

    # print(sql)
    cursor = cnx.cursor()
    cursor.execute(sql)
    cnx.commit()
    cursor.close()


if __name__ == '__main__':
    import_path = '{}/../../'.format(
        os.path.abspath(os.path.dirname(__file__)))
    sys.path.append(import_path)

    from stats.models.mysql import init_mysql
    from script_log import print_log

    try:
        print_log('starting get mysql connection...')
        hsq_cnx = init_mysql('hsq_ro')
        stats_cnx = init_mysql()

        # 获取所有未处理订单
        print_log('start dealing unshipped orders')
        to_deal = get_unshipped_order_ids(hsq_cnx)
        to_insert = get_order_detail(hsq_cnx, to_deal)
        print_log('{} unshipped orders found'.format(len(to_insert)))

        # 清空数据库
        print_log('start truncating and inserting data')
        truncate_stats(stats_cnx)

        # 插入数据库
        cols = get_columns(stats_cnx)
        for i, data in enumerate(to_insert, 1):
            print_log('inserting recorder {}/{}'.format(i, len(to_insert)))
            insert_data(stats_cnx, cols, data)

        print_log('Done!')
    except IndexError as e:
        print_log(e, 'ERROR')
    finally:
        hsq_cnx.close()
        stats_cnx.close()
