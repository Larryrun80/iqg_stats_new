#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_unshipped_order.py

import os
import sys

CONFIG_HSQ_SECTION = 'HSQ_MYSQL'
CONFIG_STATS_SECTION = 'STATS_MYSQL'
SYNC_TABLES = ('user', 'trade_order', 'merchant')


def get_columns(cnx, table_name):
    sql = 'select * from {} limit 1'.format(table_name)
    cursor = cnx.cursor()
    cursor.execute(sql)
    cols = [i[0] for i in cursor.description]
    cursor.close()
    return cols


def get_last_id(cnx, table_name):
    sql = '''
            select id
            from {}
            order by id desc
            limit 1;
          '''.format(table_name)

    cursor = cnx.cursor()
    cursor.execute(sql)
    ids = cursor.fetchall()
    cursor.close()

    if ids:
        return ids[0][0]
    else:
        return 0


def get_start_id(cnx, table_name):
    sql = '''
            select id
            from {}
            order by id asc
            limit 1;
          '''.format(table_name)

    cursor = cnx.cursor()
    cursor.execute(sql)
    ids = cursor.fetchall()
    cursor.close()

    if ids:
        return ids[0][0]
    else:
        return 0


def get_origin_data(cnx, table_name, start_id, page_size):
    data = []
    sql = '''
            select * from {table_name} where id>={start} and id<{end}
    '''.format(table_name=table_name, start=start_id, end=start_id+page_size)

    cursor = cnx.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()

    return data


def insert_data(cnx, table_name, cols, data):
    # deal data to be inserted
    escape_chars = ('\\', '"', "'")

    cursor = cnx.cursor()
    for data_line in data:
        dealed_cols = []
        dealed_data = []
        for i, d in enumerate(data_line, 0):
            if d is not None:
                dealed_cols.append(cols[i])
                if isinstance(d, int):
                    dealed_data.append(str(d))
                else:
                    sd = str(d)
                    for ec in escape_chars:
                        if ec in sd:
                            sd = sd.replace(ec, '\{}'.format(ec))
                    dealed_data.append('"{}"'.format(sd))

        ins_col = '({})'.format(','.join(dealed_cols))
        ins_val = '({})'.format(','.join(dealed_data))
        sql = '''
                insert into {table_name}
                {cols} values {vals}
              '''.format(table_name=table_name, cols=ins_col, vals=ins_val)

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
        print_log('starting get mysql connection...')
        hsq_cnx = init_mysql('hsq_ro')
        stats_cnx = init_mysql()

        page_size = 5000

        # 获取表内容并插值
        for tn in SYNC_TABLES:
            print_log('starting sync table {}'.format(tn))
            start_id = get_last_id(stats_cnx, 'hsq_{}_backup'.format(tn))
            if not start_id:
                start_id = get_start_id(hsq_cnx, tn)
            else:
                start_id += 1
            end_id = get_last_id(hsq_cnx, tn)
            cols = get_columns(hsq_cnx, tn)
            has_more = True

            while start_id <= end_id:
                data = get_origin_data(hsq_cnx, tn, start_id, page_size)
                insert_data(stats_cnx, 'hsq_{}_backup'.format(tn), cols, data)
                print_log(
                    'data with id in {} - {} inserted'.format(
                        start_id, start_id + page_size))

                start_id += page_size
            print_log('sync table {} done!'.format(tn))
    except IndexError as e:
        print_log(e, 'ERROR')
    finally:
        hsq_cnx.close()
        stats_cnx.close()
