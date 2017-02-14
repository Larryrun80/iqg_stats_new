#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_auto_tag.py

"""
    get user_tag tasks from database hsq_stats.hsq_auto_tag
    according to specific sql command recorded in each task
    get corresponding user_id from db
    and then record the user ids into hsq_stats.hsq_user_tag
"""

import os
import sys

CONFIG_HSQ_SECTION = 'HSQ_MYSQL'
CONFIG_STATS_SECTION = 'STATS_MYSQL'


def get_auto_tag_config(cnx):
    sql = '''
            select  id,
                    db,
                    sql_command,
                    tag_id,
                    comment,
                    last_sync_time,
                    creater_id
                    from hsq_auto_tag
                    where enabled = 1
        '''
    cursor = cnx.cursor()
    cursor.execute(sql)
    ids = cursor.fetchall()
    cursor.close()

    if ids[0][0]:
        return ids
    else:
        return None


def get_user_ids(cnx, command, start):

    sql = command.format(start=start)

    cursor = cnx.cursor()
    cursor.execute(sql)
    ids = cursor.fetchall()
    cursor.close()

    if ids:
        return list(ids)
    else:
        return None


def insert_data(cnx, data):
    """
        insert into hsq_stats.hsq_user_tag
    """
    sql = '''
            insert ignore into hsq_user_tag
            (`user_id`, `tag_id`, `creater_id`, `comment`, `created_at`)
            values
            (%s, %s, %s, %s, now())
        '''

    cursor = cnx.cursor()
    cursor.executemany(sql, data)
    cnx.commit()
    cursor.close()

    return True


def set_last_excute_time(cnx, task_id):
    """
        update the last_sync_time
    """
    sql = '''
            update hsq_auto_tag set last_sync_time = now() where id = {task_id}
        '''.format(task_id=task_id)

    cursor = cnx.cursor()
    cursor.execute(sql)
    cnx.commit()
    cursor.close()

    return True


if __name__ == '__main__':
    import_path = '{}/../../'.format(
        os.path.abspath(os.path.dirname(__file__)))
    sys.path.append(import_path)

    from stats.models.mysql import init_mysql
    from script_log import print_log

    try:
        stats_cnx = init_mysql()
        print_log('Start...')

        tag_configs = get_auto_tag_config(stats_cnx)

        if tag_configs:
            for i in tag_configs:
                task_id = i[0]
                db = i[1]
                sql_command = i[2]
                tag_id = i[3]
                comment = i[4]
                last_sync_time = i[5]
                creater_id = i[6]

                hsq_cnx = init_mysql(db)
                user_id = get_user_ids(hsq_cnx, sql_command, last_sync_time)
                # update the last_sync_time of auto tag task after get user_id done immediately
                set_last_excute_time(stats_cnx, task_id)

                data = []
                if user_id:
                    for user in user_id:
                        item = [user, tag_id, creater_id, comment]
                        data.append(item)
                    insert_data(stats_cnx, data)
                print_log('{} dates to be deal'.format(len(data)))

        print_log('Done!')
    except TabError as e:
        print_log(e, 'ERROR')
    finally:
        hsq_cnx.close()
        stats_cnx.close()
