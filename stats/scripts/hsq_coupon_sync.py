#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_coupon_sync.py

import json
import os
import sys

CONFIG_HSQ_SECTION = 'HSQ_MYSQL'
CONFIG_STATS_SECTION = 'STATS_MYSQL'
CREATION_ID = 0


def get_last_reward_id(cnx):
    sql = '''
            select reward_id
            from hsq_coupon_management
            order by reward_id desc
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


def get_new_coupons(cnx, reward_id):
    coupon_info = []
    sql = '''
            select reward_id,
                   coupon_id,
                   coupon_title,
                   is_multi_coupons,
                   coupons_info
            from   reward_rules
            where  reward_id > {reward_id}
            and    reward_id != 437
          '''.format(reward_id=reward_id)

    cursor = cnx.cursor()
    cursor.execute(sql)
    coupons = cursor.fetchall()
    cursor.close()

    for coupon in coupons:
        if coupon[3]:  # if is_multi_coupons
            for c in json.loads(coupon[4]):
                coupon_info.append([coupon[0],
                                   c['couponId'],
                                   str(c['couponTitle'])])
        else:
            coupon_info.append([coupon[0], coupon[1], coupon[2]])
    return coupon_info


def update_created_time(cnx, coupons):
    cursor = cnx.cursor()
    for coupon in coupons:
        sql = '''
                select from_unixtime(created_at)
                from   coupon
                where  id={}
              '''.format(coupon[1])
        cursor.execute(sql)
        coupon.append(cursor.fetchall()[0][0])

    cursor.close()
    return coupons


def update_reward(cnx, coupons):
    cursor = cnx.cursor()
    for coupon in coupons:
        sql = '''
                select title
                from   reward
                where  id={}
              '''.format(coupon[0])
        cursor.execute(sql)
        coupon.append(cursor.fetchall()[0][0])

    cursor.close()
    return coupons


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
    dealed_data.append('now()')

    ins_val = '({})'.format(','.join(dealed_data))
    sql = '''
            insert into hsq_coupon_management
            (reward_id, coupon_id, title, reward, created_at, sync_at)
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
        start_id = get_last_reward_id(stats_cnx)
        print_log('Start at id: {}'.format(start_id))

        coupons = get_new_coupons(hsq_cnx, start_id)
        print_log('{} coupons to sync'.format(len(coupons)))
        coupons = update_reward(hsq_cnx, coupons)
        coupons = update_created_time(hsq_cnx, coupons)

        for coupon in coupons:
            insert_data(stats_cnx, coupon)
        print_log('Done!')
    except Exception as e:
        print_log(e, 'ERROR')
    finally:
        if 'hsq_cnx' in locals().keys() and hsq_cnx:
            hsq_cnx.close()
        if 'stats_cnx' in locals().keys() and stats_cnx:
            stats_cnx.close()
