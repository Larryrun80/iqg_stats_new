#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_delivery_update.py

import json
import os
import sys

CONFIG_HSQ_SECTION = 'HSQ_MYSQL'
CONFIG_STATS_SECTION = 'STATS_MYSQL'
DELIVERY_STATUS_NEEDS_UPDATE = (0, 1, 2, 5, 6)


def get_orders_to_update(cnx):
    sql = '''
            select distinct(order_id)
            from   hsq_order_dealed
    '''

    cursor = cnx.cursor()
    cursor.execute(sql)
    ids = cursor.fetchall()
    cursor.close()
    return ids


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
        ids = get_orders_to_update(stats_cnx)

        # update status

    except Exception as e:
        print_log(e, 'ERROR')
    finally:
        if 'hsq_cnx' in locals().keys() and hsq_cnx:
            hsq_cnx.close()
        if 'stats_cnx' in locals().keys() and stats_cnx:
            stats_cnx.close()
