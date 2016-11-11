#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_delivery_update.py
# use hsq_logistics now

import json
import os
import sys

import arrow

CONFIG_HSQ_SECTION = 'HSQ_MYSQL'
CONFIG_STATS_SECTION = 'STATS_MYSQL'
DELIVERY_STATUS_NEEDS_UPDATE = (0, 1, 2, 5, 6)
DEFAULT_LAST_TIMESTAMP = 0


def get_last_timestamp(cnx):
    sql = '''
            select max(updated_at)
            from hsq_delivery_info
    '''

    cursor = cnx.cursor()
    cursor.execute(sql)
    ts = cursor.fetchall()
    cursor.close()

    if ts[0][0]:
        return ts[0][0]
    else:
        return DEFAULT_LAST_TIMESTAMP


def get_orders_to_update(cnx, start_time):
    sql = '''
            select      o.id,
                        m.id,
                        m.name,
                        o.status order_status,
                        dm.status delivery_status,
                        dm.delivery_com_name,
                        dm.delivery_no,
                        dm.msg,
                        dm.updated_at,
                        from_unixtime(o.created_at)
            from        `trade_order` o
            left join   delivery_message dm on o.id=dm.order_id
            inner join  merchant m on m.id=o.merchant_id
            where       dm.updated_at>{start_time}
            and         o.status in (2, 3, 5, 6, 7, 8, 9)
    '''.format(start_time=start_time)

    cursor = cnx.cursor()
    cursor.execute(sql)
    orders = cursor.fetchall()
    cursor.close()
    return orders


def get_times_from_msg(msg, delivery_status):
    package_time = None
    delivery_time = None
    finish_time = None

    if msg:
        jmsg = json.loads(msg)
        if jmsg:
            package_time = jmsg[-1]['time']

        if len(jmsg) > 1:
            delivery_time = jmsg[-2]['time']
            if delivery_status == 3:
                finish_time = jmsg[0]['time']

    return (package_time, delivery_time, finish_time)


def update_order(cnx, data):
    ins_cols = (
                    'order_id',
                    'merchant_id',
                    'merchant',
                    'order_status',
                    'delivery_status',
                    'delivery_company',
                    'delivery_no',
                    'delivery_message',
                    'updated_at',
                    'order_time',
                    'package_time',
                    'delivery_time',
                    'finish_time'
                )

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

    ins_col_val = '({})'.format(','.join(ins_cols))
    ins_val = '({})'.format(','.join(dealed_data))
    upd_tmp = []
    for i in range(1, len(ins_cols)):
        upd_tmp.append('{}={}'.format(ins_cols[i], dealed_data[i]))
    upd_val = '{}'.format(','.join(upd_tmp))

    sql = '''
            insert into hsq_delivery_info
            {ins_col} values {ins_val}
            on duplicate key update {upd_val}
    '''.format(ins_col=ins_col_val, ins_val=ins_val, upd_val=upd_val)

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

        print_log('Start')
        # get orders to update
        last_update = get_last_timestamp(stats_cnx)
        print_log('last exec updated until {}'.format(
            arrow.get(last_update).format('YYYY-MM-DD HH:mm:ss')))
        orders = list(get_orders_to_update(hsq_cnx, last_update))
        print_log('{} orders to sync'.format(len(orders)))

        # analyze and refill the order list
        for i, o in enumerate(orders, 1):
            print_log('dealing {} / {}'.format(i, len(orders)))
            o = list(o)
            if o[7]:
                o[7] = json.dumps(json.loads(o[7]), ensure_ascii=False)
            time_list = list(get_times_from_msg(o[7], o[2]))
            o = o + time_list

            # update status
            update_order(stats_cnx, o)
    except Exception as e:
        print_log(e, 'ERROR')
    finally:
        if 'hsq_cnx' in locals().keys() and hsq_cnx:
            hsq_cnx.close()
        if 'stats_cnx' in locals().keys() and stats_cnx:
            stats_cnx.close()
