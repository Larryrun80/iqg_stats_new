#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_buy_freqency.py

import os
import sys

import arrow


def get_last_order_time(cnx):
    sql = '''
            select max(last_order_time) from hsq_buy_freqency
    '''
    cursor = cnx.cursor()
    cursor.execute(sql)
    last_time = cursor.fetchone()
    cursor.close()

    if last_time[0]:
        return arrow.get(last_time[0])
    else:
        return arrow.get('2016-01-01', 'YYYY-MM-DD')


def get_update_orders(cnx, start_time):
    sql = '''
            select o.user_id, o.pay_id, o.order_at,
                   o.register_at, o.channel,
                   sum(round(if(sp.settlement_price,
                                o.sku_total_price-o.settlement_price,
                                o.sku_total_price*m.service_rate)/100,2)
                        - round(o.platform_discount/100,2))
            from hsq_order_dealed_new o
            left join hsq_sku_promotion_backup sp on o.sku_id=sp.sku_id
            left join hsq_merchant_backup m on m.id=o.merchant_id
            left join hsq_coupon_management c
                   on c.coupon_id=o.platform_coupon_id
            where order_at>'{}'
            group by pay_id
            order by order_id
    '''.format(start_time.format('YYYY-MM-DD'))
    cursor = cnx.cursor()
    cursor.execute(sql)
    orders = cursor.fetchall()
    cursor.close()

    return orders


def deal_orders(cnx, orders):
    sql = '''
            select user_id, first_order_time
            from hsq_buy_freqency
            where user_id={}
    '''
    cursor = cnx.cursor()

    for i, o in enumerate(orders, 1):
        # print('dealing {} / {}'.format(i, len(orders)))
        o = list(o)
        if o[-1] is None:
            o[-1] = 0
        order_time = arrow.get(o[2], 'Asia/Shanghai')

        t_sql = sql.format(o[0])
        cursor.execute(t_sql)
        user = cursor.fetchone()

        if user:  # exists
            first_order_time = arrow.get(user[1], 'Asia/Shanghai')
            m_diff = get_month_diff(first_order_time, order_time)
            if m_diff < 13:
                u_sql = '''
                    update hsq_buy_freqency
                    set last_order_time='{lot}',
                        {prefix}m_order_cnt={prefix}m_order_cnt+1,
                        {prefix}m_profit={prefix}m_profit+{profit}
                    where user_id={uid}
                '''.format(lot=order_time.format('YYYY-MM-DD HH:mm:ss'),
                           prefix=m_diff,
                           profit=o[-1],
                           uid=o[0])
                # print(u_sql)
                cursor.execute(u_sql)
        else:  # user not exists
            u_sql = '''
                insert into hsq_buy_freqency
                (user_id, register_at, first_order_time, last_order_time,
                 channel, 0m_order_cnt, 0m_profit)
                values
                ({uid}, '{r_date}', '{o_time}', '{o_time}', '{channel}',
                 {o_cnt}, {profit})
            '''.format(uid=o[0],
                       r_date=o[3],
                       o_time=order_time.format('YYYY-MM-DD HH:mm:ss'),
                       channel=o[-2],
                       o_cnt=1,
                       profit=o[-1])
            # print(u_sql)
            cursor.execute(u_sql)

    cnx.commit()
    cursor.close()


def get_month_diff(start_date, end_date):
    diff = -1
    for r in arrow.Arrow.span_range('month', start_date, end_date):
        diff += 1

    return diff


if __name__ == '__main__':
    import_path = '{}/../../'.format(
        os.path.abspath(os.path.dirname(__file__)))
    sys.path.append(import_path)

    from stats.models.mysql import init_mysql
    from script_log import print_log

    try:
        # hsq_cnx = init_mysql('hsq_ro')
        stats_cnx = init_mysql()

        print_log('Start...')
        start_time = get_last_order_time(stats_cnx)
        orders = get_update_orders(stats_cnx, start_time)
        deal_orders(stats_cnx, orders)

        print_log('Done!')
    except Exception as e:
        print_log(e, 'ERROR')
    finally:
        # hsq_cnx.close()
        stats_cnx.close()
