#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_comfirmed_order.py

import os
import sys

CONFIG_HSQ_SECTION = 'HSQ_MYSQL'
CONFIG_STATS_SECTION = 'STATS_MYSQL'
START_CONFIRM_DATE = 1477929600  # timestamp of 2016-11-01


def get_last_confirm_time(cnx):
    sql = '''
            select unix_timestamp(max(confirmed_at))
            from hsq_order_confirmed
          '''

    cursor = cnx.cursor()
    cursor.execute(sql)
    ids = cursor.fetchall()
    cursor.close()

    if ids[0][0]:
        return ids[0][0]
    else:
        return START_CONFIRM_DATE


def get_columns(cnx, table_name):
    sql = 'select * from {} limit 1'.format(table_name)
    cursor = cnx.cursor()
    cursor.execute(sql)
    cols = [i[0] for i in cursor.description]
    cursor.close()
    return cols


def get_orders(cnx, last_confirm):
    data = []
    sql = '''
            select  so.id sub_order_id,
                    o.id order_id,
                    o.order_type,
                    from_unixtime(o.created_at) created_at,
                    from_unixtime(o.confirm_time) confirmed_at,
                    o.merchant_id,
                    so.sku_id,
                    so.sku_name sku,
                    so.unit_price,
                    so.amount,
                    so.platform_discount,
                    so.merchant_discount,
                    sp.settlement_price,
                    toc.coupon_id,
                    if(o.order_type=3, '0', o.source_type) source
              from  trade_order o
        inner join  trade_sub_order so on o.id=so.order_id
         left join  trade_order_coupon toc on toc.order_id=o.id
         left join  sku_promotion sp on sp.sku_id=so.sku_id
             where  o.confirm_time>{confirm_time}
    '''.format(confirm_time=last_confirm)

    cursor = cnx.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()

    return data


def insert_data(cnx, cols, data):
    escape_chars = ('\\', '"', "'")
    dealed_data = []
    for d in data:
        if d is None:
            dealed_data.append('null')
        elif isinstance(d, int):
            dealed_data.append(str(d))
        else:
            sd = str(d)
            for ec in escape_chars:
                if ec in sd:
                    sd = sd.replace(ec, '\{}'.format(ec))
            dealed_data.append('"{}"'.format(sd))

    ins_val = '({})'.format(','.join(dealed_data))
    sql = '''
            insert into hsq_order_confirmed {} values {}
          '''.format(cols, ins_val)

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
        hsq_cnx = init_mysql('hsq_ro')
        stats_cnx = init_mysql()
        ct = get_last_confirm_time(stats_cnx)
        print_log('Start...')

        cols = get_columns(stats_cnx, 'hsq_order_confirmed')
        ins_cols = '({})'.format(','.join(cols))

        data = get_orders(hsq_cnx, ct)
        dealed_len = len(data)
        print_log('Totally {} orders to deal...'.format(dealed_len))

        for i, order in enumerate(data, 1):
            print_log('dealing {} / {} ...'.format(i, dealed_len))
            insert_data(stats_cnx, ins_cols, order)
        print_log('Done!')
    except TabError as e:
        print_log(e, 'ERROR')
    finally:
        hsq_cnx.close()
        stats_cnx.close()
