#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_dealed_order.py

import os
import sys

CONFIG_HSQ_SECTION = 'HSQ_MYSQL'
CONFIG_STATS_SECTION = 'STATS_MYSQL'
PAY_DURATION = 1  # unit: days


def get_start_orderid(cnx):
    return_id = 0
    cursor = cnx.cursor()

    sql = '''
            select unix_timestamp(max(order_at)) from hsq_order_dealed_new;
    '''

    cursor.execute(sql)
    ids = cursor.fetchall()

    if ids[0][0]:  # for there are 1 day payment time
        last_time = ids[0][0] - 86400 * PAY_DURATION
        sql = '''
                select order_id from hsq_order_dealed_new
                where order_at < from_unixtime({})
                order by id desc limit 1
        '''.format(last_time)

        cursor.execute(sql)
        return_ids = cursor.fetchall()
        if return_ids:
            return_id = return_ids[0][0]
    cursor.close()

    return return_id


def get_last_refund_ts(cnx):
    return_ts = 1451577600  # 2016-01-01
    cursor = cnx.cursor()

    sql = '''
            select unix_timestamp(max(refund_at)) from hsq_order_dealed_new
    '''

    cursor.execute(sql)
    ts = cursor.fetchone()

    if ts[0]:  # for there are 1 day payment time
        return_ts = ts[0]

    cursor.close()
    return return_ts


def get_traded_ids(cnx, start_id):
    ids = []
    sql = '''
            select id
            from trade_order
            where status in (2, 3, 5, 6, 7, 8, 9)
            and id > {start_id}
            order by id
          '''.format(start_id=start_id)

    cursor = cnx.cursor()
    cursor.execute(sql)
    ids = cursor.fetchall()
    cursor.close()

    return ids


def get_categories(cnx):
    categories = {}
    sql = '''
            select id, name from category
    '''
    cursor = cnx.cursor()
    cursor.execute(sql)
    cates = cursor.fetchall()
    cursor.close()

    if cates[0]:
        for (cid, name) in cates:
            categories[cid] = name

    return categories


def get_category_name(cate_string, categories):
    cate_ids = str(cate_string).split(',')
    cate_names = []
    for cid in cate_ids:
        cate_names.append(categories[int(cid)])

    return ' | '.join(cate_names)


def get_order_detail(cnx, oid):
    data = []

    # we need change category name and used the seq of col
    # so if you want to change columns here, remeber to check
    # if the translate category function works
    # check it before insert_data(stats_cnx, order_info) function
    sql = '''
             select tso.id sub_order_id,
                    o.id order_id,
                    from_unixtime(o.created_at) order_at,
                    o.pay_id pay_id,
                    m.id merchant_id,
                    m.name merchant,
                    tso.sku_id sku_id,
                    tso.sku_name sku,
                    tso.amount sku_amount,
                    tso.unit_price sku_unit_price,
                    tso.total_price sku_total_price,
                    tso.platform_discount platform_discount,
                    tso.merchant_discount merchant_discount,
                    tso.settlement_price settlement_price,
                    if(o.order_type=3, '0', o.source_type) source,
                    o.delivery_price order_delivery_price,
                    o.consignee consignee,
                    o.consignee_phone consignee_phone,
                    o.delivery_province provice,
                    o.delivery_city city,
                    o.delivery_detail_address address,
                    u.id user_id,
                    u.username username,
                    u.mobile mobile,
                    from_unixtime(u.created_at) register_at,
                    ul.register_channel channel,
                    ul.invite_user_id invite_user_id,
                    ul.last_login_ip last_login_ip,
                    pb.cate_ids categories,
                    oe.guid guid,
                    oe.device_type device_type
              from  trade_order o
        inner join  merchant m on m.id=o.merchant_id
        inner join  trade_sub_order tso on o.id=tso.order_id
        inner join  sku_basic sb on tso.sku_id=sb.id
        inner join  product_basic pb on pb.id=sb.product_id
        inner join  trade_order_ext oe on oe.order_id=o.id
        inner join  user u on u.id=o.user_id
        inner join  user_login_info ul on u.id=ul.user_id
             where  o.id={oid}
    '''.format(oid=oid)

    cursor = cnx.cursor()
    cursor.execute(sql)
    data = cursor.fetchone()
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
            insert ignore into hsq_order_dealed_new
            (sub_order_id, order_id, order_at, pay_id, merchant_id, merchant,
            sku_id, sku, sku_amount, sku_unit_price, sku_total_price,
            platform_discount, merchant_discount, settlement_price,
            source, order_delivery_price, consignee, consignee_phone,
            province, city, address, user_id,
            username, mobile, register_at, channel, invite_user_id,
            last_login_ip, categories, guid, device_type,
            platform_coupon_id, platform_coupon,
            merchant_coupon_id, merchant_coupon)
            values {}
          '''.format(ins_val)

    # print(sql)
    cursor = cnx.cursor()
    cursor.execute(sql)
    cnx.commit()
    cursor.close()


def update_pin_status(stats_cnx, hsq_cnx):
    s_cursor = stats_cnx.cursor()
    h_cursor = hsq_cnx.cursor()

    # get pin orders whose status needs update
    sql = '''
            select order_id
            from hsq_order_dealed_new
            where source=0
            and (pin_status is null or pin_status=1)
    '''
    s_cursor.execute(sql)
    po = s_cursor.fetchall()
    pin_oids = '({})'.format(','.join(
        [str(order[0]) for order in po]))

    # get status from hsq
    sql_get_status = '''
            select order_id, pin_event_status, pin_event_id
            from pin_activities_order
            where order_id in {}
    '''.format(pin_oids)
    h_cursor.execute(sql_get_status)
    s_orders = h_cursor.fetchall()

    # update status
    sql_update = '''
            update hsq_order_dealed_new
            set pin_status={ps}, pin_event_id={peid}
            where order_id={oid}
    '''
    for i, order in enumerate(s_orders, 1):
        sql_to_update = sql_update.format(ps=order[1], oid=order[0],
                                          peid=order[2])
        s_cursor.execute(sql_to_update)

    stats_cnx.commit()
    s_cursor.close()
    h_cursor.close()


def update_refund_status(stats_cnx, hsq_cnx):
    s_cursor = stats_cnx.cursor()
    h_cursor = hsq_cnx.cursor()

    last_refund_time = get_last_refund_ts(stats_cnx)
    print('refund start from {}'.format(last_refund_time))
    sql = '''
            select order_id, sub_order_id, from_unixtime(refunded_time)
            from trade_refund_order
            where refunded_time>{}
    '''.format(last_refund_time)
    h_cursor.execute(sql)
    r_orders = h_cursor.fetchall()

    for i, order in enumerate(r_orders, 1):
        if order[1]:
            sql = '''
                    update hsq_order_dealed_new
                    set refund_at='{rt}'
                    where sub_order_id={soid}
            '''.format(rt=order[2],
                       soid=order[1])
        else:
            sql = '''
                    update hsq_order_dealed_new
                    set refund_at='{rt}'
                    where order_id={oid}
            '''.format(rt=order[2],
                       oid=order[0])
        s_cursor.execute(sql)

    stats_cnx.commit()
    s_cursor.close()
    h_cursor.close()


def get_coupons(cnx, order_id):
    # with platform_coupon_id, platform_coupon,
    # merchant_coupon_id, merchant_coupon
    return_data = [0, None, 0, None]
    sql = '''
            select oc.coupon_id, c.title, c.type
            from `trade_order_coupon` oc
            inner join coupon c on oc.coupon_id=c.id
            where oc.order_id={}
    '''.format(order_id)

    cursor = cnx.cursor()
    cursor.execute(sql)
    coupons = cursor.fetchall()
    cursor.close()

    for c in coupons:
        if c[-1] and c[-1] == 1:  # platform coupon
            return_data[0] = c[0]
            return_data[1] = c[1]
        elif c[-1] and c[-1] == 2:  # merchant coupon
            return_data[2] = c[0]
            return_data[3] = c[1]

    return return_data


def update_history_coupons(stats_cnx, hsq_cnx):
    # get all orders needs to update
    sql = '''
            select order_id
            from hsq_order_dealed_new
            where (platform_discount>0 and platform_coupon_id=0)
            or (merchant_discount>0 and merchant_coupon_id=0)
    '''

    s_cursor = stats_cnx.cursor()
    s_cursor.execute(sql)
    orders = s_cursor.fetchall()

    u_sql = '''
            update hsq_order_dealed_new
            set platform_coupon_id={pci},
                platform_coupon='{pc}',
                merchant_coupon_id={mci},
                merchant_coupon='{mc}'
            where order_id={oid}
    '''

    for i, o in enumerate(orders, 1):
        print_log('dealing {} /{}'.format(i, len(orders)))
        coupons = get_coupons(hsq_cnx, o[0])
        tu_sql = u_sql.format(
            pci=coupons[0],
            pc=coupons[1],
            mci=coupons[2],
            mc=coupons[3],
            oid=o[0])
        s_cursor.execute(tu_sql)

    stats_cnx.commit()
    s_cursor.close()


if __name__ == '__main__':
    import_path = '{}/../../'.format(
        os.path.abspath(os.path.dirname(__file__)))
    sys.path.append(import_path)

    from stats.models.mysql import init_mysql
    from script_log import print_log

    try:
        hsq_cnx = init_mysql('hsq_ro')
        stats_cnx = init_mysql()
        start_id = 2148730164  # get_start_orderid(stats_cnx)

        categories = get_categories(hsq_cnx)
        print_log('Start at id: {}'.format(start_id))

        # update all paid orders
        ids = get_traded_ids(hsq_cnx, start_id)
        dealed_len = len(ids)
        print_log('Totally {} order to sync...'.format(dealed_len))

        for i, oid in enumerate(ids, 1):
            # print_log('dealing {} / {} ...'.format(i, dealed_len))
            order_info = get_order_detail(hsq_cnx, oid[0])
            if order_info:
                order_info = list(order_info)
                # add category
                order_info[-3] = get_category_name(order_info[-3], categories)
                # add coupon info
                order_info = order_info + get_coupons(hsq_cnx, oid[0])

                insert_data(stats_cnx, order_info)

        # update pin orders
        print_log("Start update pin orders status")
        update_pin_status(stats_cnx, hsq_cnx)

        # update refund
        print_log("Start update refund orders status")
        update_refund_status(stats_cnx, hsq_cnx)
        # update_history_coupons(stats_cnx, hsq_cnx)

        print_log('Done!')
    except InterruptedError as e:
        print_log(e, 'ERROR')
    finally:
        hsq_cnx.close()
        stats_cnx.close()
