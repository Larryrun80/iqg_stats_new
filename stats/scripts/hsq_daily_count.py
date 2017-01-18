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
            select max(date) from hsq_daily_statistic
    '''
    cursor = cnx.cursor()
    cursor.execute(sql)
    last_date = cursor.fetchone()
    cursor.close()

    if last_date[0]:
        return arrow.get(last_date[0])
    else:
        return arrow.get('2016-01-01', 'YYYY-MM-DD')


def get_daily_fin_data(cnx, start, end):
    '''
        pass a start date and end date(YYYY-MM-DD) and get finance data
        include gmv, gross profit, net profit, order account
        return a list format [gmv, gp, np, oa]
    '''

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
              and (o.source!=0 or o.pin_status=2)
            ) detail
    '''.format(start=start, end=end)

    cursor = cnx.cursor()
    cursor.execute(sql)
    f_data = cursor.fetchone()
    cursor.close()

    if f_data[-1] > 0:
        return list(f_data)
    return None


def get_daily_user_data(cnx, start, end):
    '''
        pass a start date and end date(YYYY-MM-DD) and get user data
        include buyer cnt, ew buyer cnt, new user cnt
        return a list format [buyer_cnt, new_buyer_cnt, user_cnt]
    '''

    # get buyer data
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
            ) detail
    '''.format(start=start, end=end)
    cursor = cnx.cursor()
    cursor.execute(sql)
    u_data = cursor.fetchone()

    # get user data
    sql = '''
            select count(0) from hsq_user_backup
            where created_at>unix_timestamp('{start}')
            and created_at<unix_timestamp('{end}')
    '''.format(start=start, end=end)
    cursor = cnx.cursor()
    cursor.execute(sql)
    u_data += cursor.fetchone()
    cursor.close()

    return list(u_data)


def insert_daily_data(cnx, data):
    '''
        pass a data with format [[date1, gmv, gp, np, oa], [date2...]]
        and insert it into database
        return true if success and raise exception if fail
    '''
    sql = '''
            insert into hsq_daily_statistic
            (`date`, `gmv`, `gross_profit`, `net_profit`, `order_cnt`,
             `customer_cnt`, `new_customer_cnt`, `registration_cnt`, `sync_at`)
            values
            (%s, %s, %s, %s, %s, %s, %s, %s, now())
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
    from script_log import print_log

    try:
        # hsq_cnx = init_mysql('hsq_ro')
        stats_cnx = init_mysql()

        print_log('Start...')
        stats_data = []
        start_date = get_last_date(stats_cnx).replace(days=1)
        end_date = arrow.now('Asia/Shanghai').replace(days=-1)

        for r in arrow.Arrow.range('day', start_date, end_date):
            daily_data = [r.format('YYYY-MM-DD')]

            start = r.format('YYYY-MM-DD')
            end = r.replace(days=1).format('YYYY-MM-DD')
            fin_data = get_daily_fin_data(stats_cnx, start, end)

            if fin_data:
                daily_data = daily_data + fin_data\
                             + get_daily_user_data(stats_cnx, start, end)

            if len(daily_data) > 1:
                stats_data.append(daily_data)

        print_log('{} dates to be deal'.format(len(stats_data)))
        insert_daily_data(stats_cnx, stats_data)

        print_log('Done!')
    except Exception as e:
        print_log(e, 'ERROR')
    finally:
        # hsq_cnx.close()
        stats_cnx.close()
