#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_user_update.py

import os
import sys

CONFIG_HSQ_SECTION = 'HSQ_MYSQL'
CONFIG_STATS_SECTION = 'STATS_MYSQL'


def get_merchants(cnx):
    sql = '''
            select id from hsq_merchant_backup
          '''

    cursor = cnx.cursor()
    cursor.execute(sql)
    ids = cursor.fetchall()
    cursor.close()

    return ids


def get_merchant_info(cnx, ids):
    merchants = []

    cursor = cnx.cursor()
    for m in ids:
        mid = m[0]
        sql = '''
                select  service_rates,
                        bd_name
                  from  merchant_ext
                 where  merchant_id={mid}
        '''.format(mid=mid)

        cursor.execute(sql)
        info = cursor.fetchall()
        if info:
            merchants.append((mid, info[0][0], info[0][1]))
    cursor.close()

    return merchants


def update_merchant(cnx, data):
    cursor = cnx.cursor()

    for m in data:
        sql = '''
            update hsq_merchant_backup
            set service_rate={rate},
                bd_name='{bd}'
            where id={mid}
        '''.format(mid=m[0], rate=m[1], bd=m[2])

        # print(sql)
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

        print_log('Start...')
        merchants = get_merchants(stats_cnx)

        if merchants:
            print_log('{} merchants to update'.format(len(merchants)))
            merchants = get_merchant_info(hsq_cnx, merchants)
            update_merchant(stats_cnx, merchants)

        print_log('Done!')
    except TabError as e:
        print_log(e, 'ERROR')
    finally:
        hsq_cnx.close()
        stats_cnx.close()
