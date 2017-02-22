#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_auto_tag.py

"""
    monitor the orders everyday, and report alarms when exception occurs
    exceptions including:
    1, too many orders (over threshold value) made by single consignee_phone
    2, to be continued
"""

import os
import sys

WARNING_THRESHOLD_VALUE = 30
MORNITOR_DATA = ['consignee_phone']
RECEIVE_MAIL = 'liyunjie@doweidu.com'
MAIL_SUBJECT = 'cheat_order_alarm'


def cheat_order_alarm(cnx, value, threshold):
    sql = '''
            select * from
            (select {value},count(0) cnt
            from hsq_order_dealed_new
            where order_at > date_format(date_sub(now(),interval 1 day),'%Y-%m-%d')
            and order_at < date_format(now(),'%Y-%m-%d')
            group by {value}) o
            where o.cnt > {threshold}
        '''.format(value=value, threshold=threshold)
    cursor = cnx.cursor()
    cursor.execute(sql)
    ids = cursor.fetchall()
    cursor.close()

    if ids:
        return list(ids)
    else:
        return None


if __name__ == '__main__':
    import_path = '{}/../../'.format(
        os.path.abspath(os.path.dirname(__file__)))
    sys.path.append(import_path)

    from stats.models.mysql import init_mysql
    from stats.models.smtp_mail import send_html_mail
    from script_log import print_log

    try:
        stats_cnx = init_mysql()
        print_log('Start...')

        content = ""
        for i in MORNITOR_DATA:
            cheat_order_users = cheat_order_alarm(stats_cnx, i, WARNING_THRESHOLD_VALUE)
            data = []
            if cheat_order_users:
                for user in cheat_order_users:
                    item = [i, user]
                    data.append(item)
                content += 'The following ' + str(i) + ' purchased over ' \
                           + str(WARNING_THRESHOLD_VALUE) + ' orders yesterday\n'
                for d in data:
                    content += str(d) + '\n'
                print_log('{} dates to be deal'.format(len(data)))

        send_html_mail(RECEIVE_MAIL, MAIL_SUBJECT, content)
        print_log('Done!')
    except TabError as e:
        print_log(e, 'ERROR')
    finally:
        # hsq_cnx.close()
        stats_cnx.close()
