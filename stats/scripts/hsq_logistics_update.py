#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_logistics_update.py

import json
import os
import sys

import arrow

CONFIG_HSQ_SECTION = 'HSQ_MYSQL'
CONFIG_STATS_SECTION = 'STATS_MYSQL'
DEFAULT_LAST_TIMESTAMP = 0
TZ = 'Asia/Shanghai'
LOGISTICS_COLUMNS = (
    'order_id',
    'pay_id',
    'order_status',
    'merchant_id',
    'delivery_status',
    'delivery_company',
    'delivery_no',
    'order_at',
    'updated_at',
    'message',
    'package_duration',
    'pickup_duration',
    'delivery_duration',
    'status',
    'dealed_at',
)


def get_last_timestamp(cnx):
    sql = '''
            select max(updated_at)
            from hsq_logistics
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
                        o.pay_id,
                        o.status order_status,
                        m.id,
                        dm.status delivery_status,
                        dm.delivery_com_name,
                        dm.delivery_no,
                        from_unixtime(o.created_at),
                        dm.updated_at,
                        from_unixtime(o.delivery_time),
                        dm.msg
            from        `trade_order` o
            left join   delivery_message dm on o.id=dm.order_id
            inner join  merchant m on m.id=o.merchant_id
            where       (dm.updated_at>{start_time} or dm.updated_at is null)
            and         o.status in (2, 3, 5, 6, 7, 8, 9)
    '''.format(start_time=start_time)
    # update deal_order function if change order of select or add/reduce fields

    cursor = cnx.cursor()
    cursor.execute(sql)
    orders = cursor.fetchall()
    cursor.close()
    return orders


def deal_order(order):
    # returns  (msg, package_duration, pickup_duration, delivery_duration)
    msg = ''
    if order[-1]:
        json_msg = json.loads(order[-1])  # msg
    else:
        json_msg = None
    order_time = order[-4]
    package_time = order[-2]
    delivery_time = None
    finish_time = None
    package_duration = None
    pickup_duration = None
    delivery_duration = None

    # deal message
    if json_msg:
        finish_time = json_msg[0]['time']
        for i in range(1, len(json_msg)+1):
            op_time = json_msg[-i]['time']
            if i == 1:
                op_time = package_time
            if i == 2:
                delivery_time = op_time
            tm = '[{time}] {msg}'.format(time=op_time,
                                         msg=json_msg[-i]['context'])
            msg += '{}\r\n'.format(tm)

    # deal duartions
    if package_time:
        package_duration = \
            arrow.get(package_time).replace(tzinfo=TZ).timestamp - \
            arrow.get(order_time).replace(tzinfo=TZ).timestamp
    else:
        package_duration = arrow.now().timestamp - \
            arrow.get(order_time).replace(tzinfo=TZ).timestamp

    if delivery_time:
        pickup_duration = \
            arrow.get(delivery_time).replace(tzinfo=TZ).timestamp - \
            arrow.get(package_time).replace(tzinfo=TZ).timestamp
    elif package_time:
        pickup_duration = arrow.now().timestamp - \
            arrow.get(package_time).replace(tzinfo=TZ).timestamp

    if order[4] == 3:  # signed
        delivery_duration = \
            arrow.get(finish_time).replace(tzinfo=TZ).timestamp - \
            arrow.get(order_time).replace(tzinfo=TZ).timestamp
    elif delivery_time:
        delivery_duration = arrow.now().timestamp - \
            arrow.get(order_time).replace(tzinfo=TZ).timestamp

    # status
    status = 0
    if order[4] == 1:
        status = 10
    if order[4] in (0, 5):
        status = 20
    if order[4] == 3:
        status = 30
    if order[4] in (2, 4, 6):
        status = 40
    return order[0:9] + (msg,  package_duration,
                         pickup_duration, delivery_duration,
                         status, arrow.now(TZ).format('YYYY-MM-DD HH:mm:ss'))


def update_order(cnx, data):
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

    ins_col_val = '({})'.format(','.join(LOGISTICS_COLUMNS))
    ins_val = '({})'.format(','.join(dealed_data))
    upd_tmp = []
    for i in range(1, len(LOGISTICS_COLUMNS)):
        upd_tmp.append('{}={}'.format(LOGISTICS_COLUMNS[i], dealed_data[i]))
    upd_val = '{}'.format(','.join(upd_tmp))

    sql = '''
            insert into hsq_logistics
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
            do = deal_order(o)
            # print(do)

            # update status
            update_order(stats_cnx, do)
    except InterruptedError as e:
        print_log(e, 'ERROR')
    finally:
        if 'hsq_cnx' in locals().keys() and hsq_cnx:
            hsq_cnx.close()
        if 'stats_cnx' in locals().keys() and stats_cnx:
            stats_cnx.close()
