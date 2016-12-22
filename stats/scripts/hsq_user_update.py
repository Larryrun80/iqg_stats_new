#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_user_update.py

import os
import sys

CONFIG_HSQ_SECTION = 'HSQ_MYSQL'
CONFIG_STATS_SECTION = 'STATS_MYSQL'


def get_users(cnx):
    sql = '''
            select id from hsq_user_backup where channel is null
          '''

    cursor = cnx.cursor()
    cursor.execute(sql)
    ids = cursor.fetchall()
    cursor.close()

    return ids


def get_channel(cnx, ids):
    users = []

    cursor = cnx.cursor()
    for u in ids:
        uid = u[0]
        sql = '''
                select  register_channel,
                        invite_user_id
                  from  user_login_info
                 where  user_id={uid}
        '''.format(uid=uid)

        cursor.execute(sql)
        ch = cursor.fetchall()
        if ch:
            users.append((uid, ch[0][0], ch[0][1]))
    cursor.close()

    return users


def update_users(cnx, data):
    cursor = cnx.cursor()

    for i, u in enumerate(data, 1):
        sql = '''
            update hsq_user_backup
            set channel='{ch}',
                inviter={inviter}
            where id={uid}
        '''.format(uid=u[0], ch=u[1], inviter=u[2])

        # print(sql)
        # print_log('dealing {} / {}...'.format(i, len(data)))
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
        users = get_users(stats_cnx)

        if users:
            print_log('{} users to update'.format(len(users)))
            users = get_channel(hsq_cnx, users)
            update_users(stats_cnx, users)

        print_log('Done!')
    except Exception as e:
        print_log(e, 'ERROR')
    finally:
        hsq_cnx.close()
        stats_cnx.close()
