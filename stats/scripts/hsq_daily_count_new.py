#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Filename: hsq_daily_count.py

import os
import sys

import arrow


def get_last_date(cnx):
    '''
        get last static date
        application will start with this date
        return an arrow obj of 2016-04-01 if no data
    '''
    sql = '''
            select max(date) from hsq_daily_statistic_new
    '''
    cursor = cnx.cursor()
    cursor.execute(sql)
    last_date = cursor.fetchone()
    cursor.close()

    if last_date[0]:
        return arrow.get(last_date[0])
    else:
        return arrow.get('2017-01-01', 'YYYY-MM-DD')


def get_daily_fin_data(cnx, start, end, daily_data):
    '''
        pass a start date and end date(YYYY-MM-DD) and get finance data
        include gmv, gross profit, net profit, order account
        return a list format [gmv, gp, np, oa]
    '''

    platforms_condition = {
        'group_app': ' and o.source=0 and o.device_type in (1,2) ',
        'group_wx': ' and o.source=0 and o.device_type=3 ',
        'jx': ' and o.source=2 ',
        'app_iOS': ' and o.source=1 and o.device_type=2 ',
        'app_android': ' and o.source=1 and o.device_type=1 ',
        'app_h5': ' and o.source=1 and o.device_type=3 ',
    }

    sql = '''
            select  sum(p*amount)+sum(dp) gmv,
                    sum(profit) gp,
                    sum(profit)-sum(pd) np,
                    count(distinct(pay_id)) order_cnt
            from (
              select  o.order_id,
                      o.pay_id,
                      round(o.sku_unit_price/100,2) p,
                      o.sku_amount amount,
                      o.settlement_price sp,
                      round(o.platform_discount/100,2) pd,
                      m.service_rate sr,
                      round(o.order_delivery_price/100,2) dp,
                      o.merchant,
                      o.sku, o.sku_id,
                      m.bd_name bd,
                      round(if(sp.settlement_price,
                               o.sku_total_price-o.settlement_price,
                               o.sku_total_price*m.service_rate)/100,2) profit
              from hsq_order_dealed_new o
              left join hsq_sku_promotion_backup sp on o.sku_id=sp.sku_id
              left join hsq_merchant_backup m on m.id=o.merchant_id
              where o.order_at>'{start}'
              and o.order_at<'{end}'
              {condition}
            ) detail
    '''
    cursor = cnx.cursor()

    for p in daily_data.keys():
        get_data = False
        if p in platforms_condition.keys():
            p_sql = sql.format(start=start, end=end,
                               condition=platforms_condition[p])
            cursor.execute(p_sql)
            f_data = cursor.fetchone()

            if f_data[0]:
                daily_data[p] += [i if i else 0 for i in f_data]
                get_data = True

        if not get_data:
            data_count = len([i for i, lt in enumerate(sql[:sql.find('from')])
                             if lt == ',']) + 1
            daily_data[p] += [0 for i in range(data_count)]

    cursor.close()

    return daily_data


def get_daily_user_data(cnx, start, end, daily_data):
    '''
        pass a start date and end date(YYYY-MM-DD) and get user data
        include buyer cnt, ew buyer cnt, new user cnt
        return a list format [buyer_cnt, new_buyer_cnt, user_cnt]
    '''

    # get buyer data

    platforms_condition = {
        'group_app': ' and source=0 and device_type in (1,2) ',
        'group_wx': ' and source=0 and device_type=3 ',
        'jx': ' and source=2 ',
        'app_iOS': ' and source=1 and device_type=2 ',
        'app_android': ' and source=1 and device_type=1 ',
        'app_h5': ' and source=1 and device_type=3 ',
    }

    # condition indicate this order belongs to normal user
    ws_condition = {
       'normal': ' and (mobile="" or mobile=consignee_phone) ',
       'ws': 'and (mobile!="" and mobile!=consignee_phone)',
    }

    sql = '''
            select count(0) total_buyer, count(new_buyer) new_buyer
            from (
              select distinct(user_id),
                     if(user_id not in (select distinct(user_id)
                                        from hsq_order_dealed_new
                                        where order_at<'{start}'),
                                        'new', null) new_buyer
              from hsq_order_dealed_new
              where order_at>'{start}'
              and order_at<'{end}'
              {condition}
              {ws_condition}
            ) detail
    '''
    cursor = cnx.cursor()

    for p in daily_data.keys():
        customer_data = {
            'normal_cnt': 0,
            'ws_cnt': 0,
            'new_normal_cnt': 0,
            'new_ws_cnt': 0,
        }

        if p in platforms_condition.keys():
            for ws in ws_condition.keys():
                p_sql = sql.format(start=start,
                                   end=end,
                                   condition=platforms_condition[p],
                                   ws_condition=ws_condition[ws])
                cursor.execute(p_sql)
                f_data = cursor.fetchone()

                if f_data:
                    customer_data['{}_cnt'.format(ws)] += f_data[0]
                    customer_data['new_{}_cnt'.format(ws)] += f_data[1]

        daily_data[p] += [customer_data['normal_cnt'],
                          customer_data['ws_cnt'],
                          customer_data['new_normal_cnt'],
                          customer_data['new_ws_cnt']]

    cursor.close()

    return daily_data


def get_daily_uv_data(cnx, start, daily_data):
    db = cnx['online_api_analyse']
    collection = db['guid']
    cursor = collection.find({'date': start[2:]})
    result = []
    for doc in cursor:
        result.append(doc)

    daily_data_map = {
                'group_app': 'couple',
                'jx': 'jingxuan',
                'app_iOS': 'ios',
                'app_android': 'android',
                'app_h5': 'wap'
            }
    uv_data = {}

    for k, v in daily_data_map.items():
        for item in result:
            if item['terminal'] == v:
                uv_data[k] += [item['totalCnt'], item['validCnt']]
                break

    for key in daily_data.keys():
        if key not in uv_data.keys():
            daily_data[key] += [0, 0]
        else:
            daily_data[key] += uv_data[key]

    return daily_data


def add_total(cnx, start, end, daily_data):
    ''' add column registration_cnt and add 'total' row
        col data of total will be sum of all platform
        registration_cnt data of platform row will be 0
    '''

    total = []
    for key in daily_data.keys():
        if not total:
            total = [i for i in daily_data[key]]
        else:
            for i in range(len(total)):
                total[i] += daily_data[key][i]
        # registration_cnt value
        daily_data[key] += [0]

    cursor = cnx.cursor()
    # get registeration
    sql = '''
            select count(0) from hsq_user_backup
            where created_at>unix_timestamp('{start}')
            and created_at<unix_timestamp('{end}')
    '''.format(start=start, end=end)
    cursor.execute(sql)
    reg_cnt = cursor.fetchone()

    cursor.close()

    if reg_cnt[0]:
        total += reg_cnt
    else:
        total += [0]

    daily_data['total'] = total

    return daily_data


def insert_daily_data(cnx, data):
    '''
        pass a data with format [[date1, gmv, gp, np, oa], [date2...]]
        and insert it into database
        return true if success and raise exception if fail
    '''
    sql = '''
            insert into hsq_daily_statistic_new
            (`date`, `platform`, `uv`, `active_uv`, `gmv`, `gross_profit`,
             `net_profit`, `order_cnt`, `customer_normal_cnt`,
             `customer_ws_cnt`, `new_customer_normal_cnt`,
             `new_customer_ws_cnt`, `registration_cnt`, `sync_at`)
            values
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
    '''

    cursor = cnx.cursor()
    cursor.executemany(sql, data)
    cnx.commit()
    cursor.close()

    return True


if __name__ == '__main__':
    import_path = '{}/../../'.format(
        os.path.abspath(os.path.dirname(__file__)))
    sys.path.append(import_path)

    from stats.models.mysql import init_mysql
    from stats.models.mongo import init_mongo
    from script_log import print_log

    try:
        # hsq_cnx = init_mysql('hsq_ro')
        stats_cnx = init_mysql()
        office_mongo_cnx = init_mongo('HSQ_OFFICE_MONGO')

        print_log('Start...')
        stats_data = []
        start_date = get_last_date(stats_cnx).replace(days=1)
        end_date = arrow.now('Asia/Shanghai').replace(days=-1)

        for r in arrow.Arrow.range('day', start_date, end_date):
            # define platform data and init
            daily_data = {
                'group_app': [],
                'group_wx': [],
                'jx': [],
                'app_iOS': [],
                'app_android': [],
                'app_h5': []
            }

            start = r.format('YYYY-MM-DD')
            end = r.replace(days=1).format('YYYY-MM-DD')
            print_log('start dealing date {}'.format(start))
            daily_data = get_daily_uv_data(office_mongo_cnx, start, daily_data)
            print_log('  - uv data got')
            daily_data = get_daily_fin_data(stats_cnx, start, end, daily_data)
            print_log('  - fin data got')
            daily_data = get_daily_user_data(stats_cnx, start, end, daily_data)
            print_log('  - user data got')
            daily_data = add_total(stats_cnx, start, end, daily_data)
            print_log('  - total done')

            for k in daily_data.keys():
                stats_data.append([r.format('YYYY-MM-DD'), k] +
                                  [float(i) for i in daily_data[k]])

        insert_daily_data(stats_cnx, stats_data)

        print_log('Done!')
    except Exception as e:
        print_log(e, 'ERROR')
    finally:
        # hsq_cnx.close()
        stats_cnx.close()
        office_mongo_cnx.close()
