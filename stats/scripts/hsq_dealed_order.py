#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_dealed_order.py

import os
import sys

CONFIG_HSQ_SECTION = 'HSQ_MYSQL'
CONFIG_STATS_SECTION = 'STATS_MYSQL'
CREATION_ID = 2147486532


def get_last_order_id(cnx):
    sql = '''
            select order_id
            from hsq_order_dealed
            order by order_id desc
            limit 1;
          '''

    cursor = cnx.cursor()
    cursor.execute(sql)
    ids = cursor.fetchall()
    cursor.close()

    if ids:
        return ids[0][0]
    else:
        return CREATION_ID


def get_traded_ids(cnx, start_id):
    ids = []
    sql = '''
            select id
            from trade_order
            where status in (2, 3, 5, 6, 7, 8, 9)
            and id > {start_id}
          '''.format(start_id=start_id)

    cursor = cnx.cursor()
    cursor.execute(sql)
    ids = cursor.fetchall()
    cursor.close()

    return ids


def get_order_detail(cnx, oid):
    data = []
    sql = '''
            select  o.id order_id,
                    from_unixtime(o.created_at) order_at,
                    o.pay_id pay_id,
                    m.id merchant_id,
                    m.name merchant,
                    tso.sku_id sku_id,
                    tso.sku_name sku,
                    tso.amount sku_amount,
                    tso.total_price sku_total,
                    tso.discount_price sku_discount,
                    o.delivery_price order_delivery_price,
                    o.consignee consignee,
                    o.consignee_phone consignee_phone,
                    o.delivery_province provice,
                    o.delivery_city city,
                    o.delivery_detail_address address,
                    if(c.id, c.id, 0) coupon_id,
                    c.title coupon,
                    u.id user_id,
                    u.username username,
                    u.mobile mobile,
                    from_unixtime(u.created_at) register_at,
                    ul.register_channel channel,
                    ul.invite_user_id invite_user_id,
                    ul.last_login_ip last_login_ip
              from  trade_order o
        inner join  merchant m on m.id=o.merchant_id
        inner join  trade_sub_order tso on o.id=tso.order_id
         left join  trade_order_coupon toc on toc.order_id=o.id
         left join  coupon c on c.id=toc.coupon_id
        inner join  user u on u.id=o.user_id
        inner join  user_login_info ul on u.id=ul.user_id
             where  o.id={oid}
    '''.format(oid=oid)

    cursor = cnx.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()

    return data


def insert_data(cnx, data):
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

    ins_val = '({})'.format(','.join(dealed_data))
    sql = '''
            insert into hsq_order_dealed
            (order_id, order_at, pay_id, merchant_id, merchant,
            sku_id, sku, sku_amount, sku_total, sku_discount,
            order_delivery_price, consignee, consignee_phone,
            province, city, address, coupon_id, coupon, user_id,
            username, mobile, register_at, channel, invite_user_id,
            last_login_ip)
            values {}
          '''.format(ins_val)

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
        start_id = get_last_order_id(stats_cnx)
        print_log('Start at id: {}'.format(start_id))

        ids = get_traded_ids(hsq_cnx, start_id)
        dealed_len = len(ids)
        print_log('Totally {} order to sync...'.format(dealed_len))

        for i, oid in enumerate(ids, 1):
            print_log('dealing {} / {} ...'.format(i, dealed_len))
            order_info = get_order_detail(hsq_cnx, oid[0])
            for so in order_info:
                insert_data(stats_cnx, so)
        print_log('Done!')
    except Exception as e:
        print_log(e, 'ERROR')
    finally:
        hsq_cnx.close()
        stats_cnx.close()
